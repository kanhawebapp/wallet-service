// src/consumer.js

import "dotenv/config";
import amqp from "amqplib";

import pkg from "@prisma/client";
const { PrismaClient } = pkg;

import pg from "pg";
const { Pool } = pg;

import { PrismaPg } from "@prisma/adapter-pg";

/**
 * PostgreSQL Pool
 */
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 10,
});

/**
 * Prisma Adapter
 */
const adapter = new PrismaPg(pool);

/**
 * Prisma Client
 */
const prisma = new PrismaClient({
  adapter,
  log: ["error", "warn"],
});

let connection;
let channel;

const QUEUE_NAME = "payment.success";

/**
 * ============================
 * CONFIGURATION
 * ============================
 */

const GST_RATE = Number(process.env.GST_RATE || 18);

const COMPANY_STATE = process.env.COMPANY_STATE || "Delhi";

const PG_RATE = Number(process.env.PG_RATE || 1.65);

const PG_GST_RATE = Number(process.env.PG_GST_RATE || 18);

/**
 * ============================
 * CALCULATE GST
 * ============================
 *
 * Amount received from Razorpay is GST inclusive.
 */
function calculateGST(totalAmount, customerState) {
  console.log("GST calculation totalAmount:", totalAmount);

  const numericTotalAmount = Number(totalAmount || 0);

  const taxableAmount = Number(
    ((numericTotalAmount * 100) / (100 + GST_RATE)).toFixed(2),
  );

  const totalTax = Number((numericTotalAmount - taxableAmount).toFixed(2));

  console.log("GST taxableAmount:", taxableAmount);
  console.log("GST totalTax:", totalTax);

  let cgst = 0;
  let sgst = 0;
  let igst = 0;

  if (
    customerState &&
    customerState.trim().toLowerCase() === COMPANY_STATE.trim().toLowerCase()
  ) {
    cgst = Number((totalTax / 2).toFixed(2));
    sgst = Number((totalTax / 2).toFixed(2));
  } else {
    igst = totalTax;
  }

  return {
    taxableAmount,
    gstRate: GST_RATE,
    cgst,
    sgst,
    igst,
    totalTax,
    totalAmount: numericTotalAmount,
  };
}

/**
 * ============================
 * CALCULATE PAYMENT GATEWAY
 * CHARGES
 * ============================
 */

function calculatePGCharges(totalAmount) {
  const numericTotalAmount = Number(totalAmount || 0);

  const pgCharge = Number((numericTotalAmount * (PG_RATE / 100)).toFixed(2));

  const pgIgst = Number((pgCharge * (PG_GST_RATE / 100)).toFixed(2));

  const pgTotal = Number((pgCharge + pgIgst).toFixed(2));

  const receivableAmount = Number((numericTotalAmount - pgTotal).toFixed(2));

  return {
    pgChargeRate: PG_RATE,
    pgCharge,
    pgIgst,
    pgTotal,
    receivableAmount,
  };
}

/**
 * ============================
 * SERVICE COUPON REDEMPTION
 * ============================
 *
 * IMPORTANT:
 *
 * Service bookings use ServicePaymentOrder.
 *
 * Recharge uses PaymentOrder.
 *
 * Therefore:
 *
 * SERVICE:
 * CouponRedemption.servicePaymentOrderId
 *
 * RECHARGE:
 * CouponRedemption.paymentOrderId
 */
async function redeemServiceCoupon(tx, data, servicePaymentOrder) {
  console.log("dataaaaaaaaaaaaaaaaaaaaaaaaaaaaa", data);
  if (!data.couponCode || data.couponCode.trim() === "") {
    return;
  }

  console.log("Processing SERVICE coupon redemption:", {
    couponCode: data.couponCode,
    couponType: data.couponType,
    userId: data.userId,
    discount: data.discount,
    cashback: data.cashback,
  });

  /**
   * ============================
   * FIND COUPON
   * ============================
   */

  const coupon = await tx.coupon.findUnique({
    where: {
      code: data.couponCode.trim().toUpperCase(),
    },
  });

  if (!coupon) {
    throw new Error(`Coupon not found: ${data.couponCode}`);
  }

  /**
   * ============================
   * CHECK USER REDEMPTION
   * ============================
   */

  const existingRedemption = await tx.couponRedemption.findFirst({
    where: {
      couponId: coupon.id,
      userId: data.userId,
    },
  });

  if (existingRedemption) {
    throw new Error(
      `Coupon ${coupon.code} has already been redeemed by this user`,
    );
  }

  /**
   * ============================
   * CHECK GLOBAL COUPON LIMIT
   * ============================
   */

  if (
    coupon.redeemLimit !== null &&
    coupon.redeemLimit !== undefined &&
    Number(coupon.usedCount || 0) >= Number(coupon.redeemLimit)
  ) {
    throw new Error(`Coupon ${coupon.code} redemption limit has been reached`);
  }

  /**
   * ============================
   * CREATE SERVICE REDEMPTION
   * ============================
   *
   * DO NOT use:
   *
   * paymentOrderId: servicePaymentOrder.id
   *
   * because this is NOT a PaymentOrder.
   *
   * Service payment goes into:
   *
   * servicePaymentOrderId
   */

  await tx.couponRedemption.create({
    data: {
      couponId: coupon.id,
      userId: data.userId,
      servicePaymentOrderId: servicePaymentOrder.id,
      discount: Number(data.discount || 0),
    },
  });

  /**
   * ============================
   * INCREMENT COUPON USAGE
   * ============================
   */

  await tx.coupon.update({
    where: {
      id: coupon.id,
    },
    data: {
      usedCount: {
        increment: 1,
      },
    },
  });

  console.log(
    `SERVICE COUPON REDEEMED SUCCESSFULLY: coupon=${coupon.code}, couponId=${coupon.id}, user=${data.userId}`,
  );
}

/**
 * ============================
 * RECHARGE COUPON REDEMPTION
 * ============================
 *
 * Recharge coupons are stored directly
 * inside PaymentOrder.couponId.
 */
async function redeemRechargeCoupon(tx, data, paymentOrder) {
  /**
   * No coupon applied
   */
  if (!paymentOrder.couponId) {
    return;
  }

  console.log("Processing RECHARGE coupon redemption:", {
    couponId: paymentOrder.couponId,
    couponCode: data.couponCode,
    couponType: data.couponType,
    userId: data.userId,
    discount: data.discount,
    cashback: data.cashback,
  });

  /**
   * ============================
   * FIND COUPON
   * ============================
   */

  const coupon = await tx.coupon.findUnique({
    where: {
      id: paymentOrder.couponId,
    },
  });

  if (!coupon) {
    throw new Error(
      `Coupon not found for PaymentOrder: ${paymentOrder.couponId}`,
    );
  }

  /**
   * ============================
   * CHECK USER REDEMPTION
   * ============================
   */

  const existingRedemption = await tx.couponRedemption.findFirst({
    where: {
      couponId: coupon.id,
      userId: data.userId,
    },
  });

  /**
   * If already redeemed, do not create again.
   *
   * This makes the RabbitMQ consumer idempotent
   * for coupon redemption.
   */
  if (existingRedemption) {
    console.log(
      `COUPON ALREADY REDEEMED: coupon=${coupon.code}, couponId=${coupon.id}, user=${data.userId}`,
    );

    return;
  }

  /**
   * ============================
   * CHECK GLOBAL LIMIT
   * ============================
   */

  if (
    coupon.redeemLimit !== null &&
    coupon.redeemLimit !== undefined &&
    Number(coupon.usedCount || 0) >= Number(coupon.redeemLimit)
  ) {
    throw new Error(`Coupon ${coupon.code} redemption limit has been reached`);
  }

  /**
   * ============================
   * CREATE RECHARGE REDEMPTION
   * ============================
   */

  await tx.couponRedemption.create({
    data: {
      couponId: paymentOrder.couponId,
      userId: data.userId,
      paymentOrderId: paymentOrder.id,
      discount: Number(data.discount || 0),
    },
  });

  /**
   * ============================
   * INCREMENT COUPON USAGE
   * ============================
   */

  await tx.coupon.update({
    where: {
      id: coupon.id,
    },
    data: {
      usedCount: {
        increment: 1,
      },
    },
  });

  console.log(
    `RECHARGE COUPON REDEEMED SUCCESSFULLY: coupon=${coupon.code}, couponId=${coupon.id}, user=${data.userId}`,
  );
}

/**
 * ============================
 * START CONSUMER
 * ============================
 */

async function startConsumer() {
  try {
    console.log("Connecting to RabbitMQ...");

    connection = await amqp.connect(process.env.RABBITMQ_URL);

    connection.on("error", (err) => {
      console.error("RabbitMQ connection error:", err.message);
    });

    connection.on("close", () => {
      console.warn("RabbitMQ connection closed. Reconnecting...");

      setTimeout(startConsumer, 5000);
    });

    channel = await connection.createChannel();

    /**
     * ============================
     * ENSURE QUEUE EXISTS
     * ============================
     */

    await channel.assertQueue(QUEUE_NAME, {
      durable: true,
    });

    /**
     * ============================
     * PREVENT PARALLEL PROCESSING
     * ============================
     */

    channel.prefetch(1);

    console.log(`Waiting for messages in queue: ${QUEUE_NAME}`);

    /**
     * ============================
     * CONSUME PAYMENT MESSAGE
     * ============================
     */

    channel.consume(
      QUEUE_NAME,
      async (msg) => {
        if (!msg) return;

        let data;

        /**
         * ============================
         * PARSE RABBITMQ MESSAGE
         * ============================
         */

        try {
          data = JSON.parse(msg.content.toString());
        } catch (err) {
          console.error("Invalid JSON message", err);

          channel.nack(msg, false, false);

          return;
        }

        console.log("Received payment.success message:", data);

        try {
          /**
           * ============================
           * PRISMA TRANSACTION
           * ============================
           */

          await prisma.$transaction(
            async (tx) => {
              /**
               * =================================================
               * SERVICE PAYMENT
               * =================================================
               */

              /**
               * =================================================
               * SERVICE PAYMENT
               * =================================================
               */

              if (data.serviceType === "SERVICE") {
                /**
                 * =================================================
                 * FIND SERVICE PAYMENT ORDER
                 * =================================================
                 */

                const servicePaymentOrder =
                  await tx.servicePaymentOrder.findUnique({
                    where: {
                      razorpayOrderId: data.orderId,
                    },
                  });

                if (!servicePaymentOrder) {
                  throw new Error(
                    `Service payment order not found: ${data.orderId}`,
                  );
                }

                /**
                 * =================================================
                 * FAILED SERVICE PAYMENT
                 * =================================================
                 */

                if (data.status === "failed") {
                  await tx.servicePaymentOrder.update({
                    where: {
                      id: servicePaymentOrder.id,
                    },
                    data: {
                      status: "FAILED",
                    },
                  });

                  console.log(`SERVICE PAYMENT FAILED: order=${data.orderId}`);

                  return;
                }

                /**
                 * =================================================
                 * ONLY PROCESS CAPTURED PAYMENT
                 * =================================================
                 */

                if (data.status !== "captured") {
                  console.log(
                    `Ignoring unsupported service payment status: ${data.status}`,
                  );

                  return;
                }

                /**
                 * =================================================
                 * PREVENT DUPLICATE PAYMENT PROCESSING
                 * =================================================
                 */

                if (servicePaymentOrder.status === "PAID") {
                  console.log(
                    `Service payment already processed: ${data.orderId}`,
                  );

                  return;
                }

                /**
                 * =================================================
                 * FIND SERVICE COUPON
                 * =================================================
                 *
                 * couponCode comes from the payment.success event.
                 *
                 * couponId in ServicePaymentOrder must contain
                 * the actual Coupon.id, NOT the coupon code.
                 */

                let couponId = null;

                if (data.couponCode && data.couponCode.trim() !== "") {
                  const coupon = await tx.coupon.findUnique({
                    where: {
                      code: data.couponCode.trim().toUpperCase(),
                    },
                    select: {
                      id: true,
                      code: true,
                      type: true,
                    },
                  });

                  if (!coupon) {
                    throw new Error(`Coupon not found: ${data.couponCode}`);
                  }

                  couponId = coupon.id;

                  console.log("SERVICE COUPON FOUND:", {
                    couponId: coupon.id,
                    couponCode: coupon.code,
                    couponType: coupon.type,
                  });
                }

                /**
                 * =================================================
                 * NORMALIZE PAYMENT VALUES
                 * =================================================
                 */

                const totalAmount = Number(data.coins || 0);

                const payableAmount = Number(data.amount || 0);

                const discount = Number(data.discount || 0);

                const cashback = Number(data.cashback || 0);

                console.log("SERVICE PAYMENT VALUES:", {
                  orderId: data.orderId,
                  paymentId: data.paymentId,
                  totalAmount,
                  payableAmount,
                  couponId,
                  discount,
                  cashback,
                });

                /**
                 * =================================================
                 * UPDATE SERVICE PAYMENT ORDER
                 * =================================================
                 *
                 * Mapping:
                 *
                 * data.coins       -> totalAmount
                 * data.amount      -> payableAmount
                 * coupon.id        -> couponId
                 * data.discount    -> discount
                 * data.cashback    -> cashback
                 * captured         -> PAID
                 */

                await tx.servicePaymentOrder.update({
                  where: {
                    id: servicePaymentOrder.id,
                  },
                  data: {
                    totalAmount,
                    payableAmount,
                    couponId,
                    discount,
                    cashback,
                    status: "PAID",
                  },
                });

                console.log(
                  `SERVICE PAYMENT ORDER UPDATED: order=${data.orderId}`,
                );

                /**
                 * =================================================
                 * UPDATE SERVICE BOOKING
                 * =================================================
                 */

                await tx.serviceBooking.update({
                  where: {
                    id: servicePaymentOrder.bookingId,
                  },
                  data: {
                    paymentStatus: "SUCCESS",
                    bookingStatus: "ASSIGNED",
                  },
                });

                /**
                 * =================================================
                 * SERVICE COUPON REDEMPTION
                 * =================================================
                 */

                if (couponId) {
                  await redeemServiceCoupon(tx, data, {
                    ...servicePaymentOrder,
                    couponId,
                  });
                }

                /**
                 * =================================================
                 * SERVICE SUCCESS
                 * =================================================
                 */

                console.log(
                  `SERVICE PAYMENT SUCCESS: booking=${servicePaymentOrder.bookingId}, payment=${servicePaymentOrder.id}, totalAmount=${totalAmount}, payableAmount=${payableAmount}, coupon=${data.couponCode || "NONE"}, discount=${discount}, cashback=${cashback}`,
                );

                return;
              }

              if (message.serviceType === "PUJA") {
                const orderId = message.orderId;

                if (!orderId) {
                  throw new Error(
                    "Missing Razorpay order ID in payment-success event",
                  );
                }

                const pujaOrder = await prisma.pujaPaymentOrder.findUnique({
                  where: {
                    razorpayOrderId: orderId,
                  },
                  include: {
                    coupon: true,
                  },
                });

                if (!pujaOrder) {
                  throw new Error(`Puja payment order not found: ${orderId}`);
                }

                // Read amounts from the database.
                const discount = Number(pujaOrder.discount ?? 0);
                const cashback = Number(pujaOrder.cashback ?? 0);
                const totalAmount = Number(pujaOrder.totalAmount);
                const payableAmount = Number(pujaOrder.payableAmount);

                const couponCode = pujaOrder.coupon?.code ?? null;
                const couponType = pujaOrder.coupon?.type ?? null;

                console.log("Processing PUJA payment:", {
                  orderId,
                  bookingId: pujaOrder.bookingId,
                  userId: pujaOrder.userId,
                  totalAmount,
                  payableAmount,
                  discount,
                  cashback,
                  couponCode,
                  couponType,
                  status: pujaOrder.status,
                });

                // Validate payment amounts.
                if (
                  !Number.isFinite(totalAmount) ||
                  !Number.isFinite(payableAmount) ||
                  !Number.isFinite(discount) ||
                  !Number.isFinite(cashback) ||
                  totalAmount < 0 ||
                  payableAmount < 0 ||
                  discount < 0 ||
                  cashback < 0
                ) {
                  throw new Error(
                    `Invalid payment amounts for order: ${orderId}`,
                  );
                }

                // Prevent processing an already completed payment.
                if (pujaOrder.status === "SUCCESS") {
                  console.log("PUJA payment already processed:", orderId);
                  return;
                }

                /*
                 * Cashback is credited only when:
                 * 1. A coupon is associated with the payment order.
                 * 2. The coupon type is CASHBACK.
                 * 3. The cashback amount is greater than zero.
                 */
                const isCashbackCoupon =
                  pujaOrder.coupon != null &&
                  String(pujaOrder.coupon.type).toUpperCase() === "CASHBACK";

                const cashbackToCredit =
                  isCashbackCoupon && cashback > 0 ? cashback : 0;

                console.log("PUJA cashback eligibility:", {
                  orderId,
                  couponCode,
                  couponType,
                  isCashbackCoupon,
                  cashbackToCredit,
                });

                const result = await prisma.$transaction(async (tx) => {
                  /*
                   * Claim the payment order atomically.
                   * Concurrent duplicate RabbitMQ messages must not credit
                   * cashback multiple times.
                   */
                  const claimedOrder = await tx.pujaPaymentOrder.updateMany({
                    where: {
                      id: pujaOrder.id,
                      status: {
                        not: "SUCCESS",
                      },
                    },
                    data: {
                      status: "SUCCESS",
                    },
                  });

                  if (claimedOrder.count === 0) {
                    return {
                      alreadyProcessed: true,
                      orderId,
                    };
                  }

                  let previousBalance = null;
                  let updatedBalance = null;

                  // Credit cashback to the user's wallet only for cashback coupons.
                  if (cashbackToCredit > 0) {
                    const wallet = await tx.wallet.findUnique({
                      where: {
                        userId: pujaOrder.userId,
                      },
                    });

                    if (!wallet) {
                      throw new Error(
                        `Wallet not found for user: ${pujaOrder.userId}`,
                      );
                    }

                    previousBalance = Number(wallet.balance);

                    const updatedWallet = await tx.wallet.update({
                      where: {
                        userId: pujaOrder.userId,
                      },
                      data: {
                        balance: {
                          increment: cashbackToCredit,
                        },
                      },
                    });

                    updatedBalance = Number(updatedWallet.balance);

                    /*
                     * Record the cashback transaction using your existing
                     * wallet transaction model and fields.
                     */
                    await tx.walletTransaction.create({
                      data: {
                        userId: pujaOrder.userId,
                        walletId: wallet.id,
                        amount: cashbackToCredit,
                        type: "CASHBACK",
                        status: "SUCCESS",
                        description: `Puja cashback using coupon ${couponCode}`,
                        referenceId: pujaOrder.razorpayOrderId,
                      },
                    });

                    console.log("PUJA cashback successfully credited:", {
                      userId: pujaOrder.userId,
                      orderId,
                      couponCode,
                      cashback: cashbackToCredit,
                      previousBalance,
                      updatedBalance,
                    });
                  } else {
                    console.log("No cashback credit required:", {
                      orderId,
                      couponCode,
                      couponType,
                      cashback,
                    });
                  }

                  return {
                    alreadyProcessed: false,
                    orderId,
                    bookingId: pujaOrder.bookingId,
                    userId: pujaOrder.userId,
                    totalAmount,
                    payableAmount,
                    discount,
                    cashback: cashbackToCredit,
                    couponCode,
                    couponType,
                    previousBalance,
                    updatedBalance,
                  };
                });

                if (result.alreadyProcessed) {
                  console.log("PUJA payment already processed:", orderId);
                  return;
                }

                console.log("PUJA payment processed successfully:", result);
              }

              /**
               * =================================================
               * RECHARGE / NORMAL WALLET PAYMENT
               * =================================================
               *
               * Any payment which is NOT SERVICE
               * is treated as recharge.
               */

              const paymentOrder = await tx.paymentOrder.findUnique({
                where: {
                  razorpayOrderId: data.orderId,
                },
              });

              if (!paymentOrder) {
                throw new Error(`Payment order not found: ${data.orderId}`);
              }

              /**
               * =================================================
               * FAILED PAYMENT
               * =================================================
               */

              if (data.status === "failed") {
                await tx.paymentOrder.update({
                  where: {
                    id: paymentOrder.id,
                  },
                  data: {
                    status: "FAILED",
                  },
                });

                console.log(
                  `FAILED PAYMENT: order=${data.orderId}, payment=${data.paymentId}`,
                );

                return;
              }

              /**
               * =================================================
               * ONLY HANDLE CAPTURED
               * =================================================
               */

              if (data.status !== "captured") {
                console.log(
                  `Ignoring unsupported payment status: ${data.status}`,
                );

                return;
              }

              /**
               * =================================================
               * PREVENT DUPLICATE PAYMENT PROCESSING
               * =================================================
               */

              const existingPayment = await tx.payment.findUnique({
                where: {
                  razorpayPaymentId: data.paymentId,
                },
              });

              if (existingPayment) {
                console.log(`Payment already processed: ${data.paymentId}`);

                return;
              }

              /**
               * =================================================
               * UPDATE PAYMENT ORDER
               * =================================================
               */

              await tx.paymentOrder.update({
                where: {
                  id: paymentOrder.id,
                },
                data: {
                  status: "PAID",
                },
              });

              /**
               * =================================================
               * CALCULATE GST
               * =================================================
               */

              const gst = calculateGST(data.amount, data.state);

              /**
               * =================================================
               * CALCULATE PG CHARGES
               * =================================================
               */

              const pg = calculatePGCharges(data.amount);

              /**
               * =================================================
               * GENERATE INVOICE NUMBER
               * =================================================
               */

              const invoiceNo = `INV-${new Date().getFullYear()}-${Date.now()}`;

              console.log("taxableAmount:", gst.taxableAmount);

              /**
               * =================================================
               * CREATE PAYMENT RECORD
               * =================================================
               */

              const payment = await tx.payment.create({
                data: {
                  userId: data.userId,

                  rechargePackId: data.rechargePackId,

                  paymentOrderId: paymentOrder.id,

                  amount: data.amount,

                  coins: data.coins,

                  provider: "RAZORPAY",

                  razorpayOrderId: data.orderId,

                  razorpayPaymentId: data.paymentId,

                  status: "SUCCESS",

                  invoiceNo,

                  taxableAmount: gst.taxableAmount,

                  gstRate: gst.gstRate,

                  cgst: gst.cgst,

                  sgst: gst.sgst,

                  igst: gst.igst,

                  totalTax: gst.totalTax,

                  totalAmount: gst.totalAmount,

                  pgChargeRate: pg.pgChargeRate,

                  pgCharge: pg.pgCharge,

                  pgIgst: pg.pgIgst,

                  pgTotal: pg.pgTotal,

                  receivableAmount: pg.receivableAmount,

                  country: data.country,

                  state: data.state,

                  city: data.city,

                  platform: data.platform,
                },
              });

              /**
               * =================================================
               * RECHARGE COUPON REDEMPTION
               * =================================================
               *
               * IMPORTANT:
               *
               * We use paymentOrder.couponId.
               *
               * NOT data.couponId.
               *
               * Because createOrder already stores:
               *
               * couponId: coupon?.id || null
               */

              await redeemRechargeCoupon(tx, data, paymentOrder);

              /**
               * =================================================
               * WALLET CREDIT
               * =================================================
               *
               * Only recharge coins are credited here.
               */

              const totalCoins = Number(data.coins || 0);

              const wallet = await tx.userWallet.upsert({
                where: {
                  userId: data.userId,
                },

                update: {
                  balanceCoins: {
                    increment: totalCoins,
                  },
                },

                create: {
                  userId: data.userId,

                  balanceCoins: totalCoins,

                  lockedCoins: 0,
                },
              });

              /**
               * =================================================
               * PREVENT DUPLICATE WALLET TX
               * =================================================
               */

              const existingWalletTx = await tx.walletTransaction.findFirst({
                where: {
                  paymentId: payment.id,

                  type: "CREDIT",
                },
              });

              if (existingWalletTx) {
                console.log(
                  `Wallet transaction already exists for payment=${payment.id}`,
                );

                return;
              }

              /**
               * =================================================
               * CREATE CREDIT TRANSACTION
               * =================================================
               */

              await tx.walletTransaction.create({
                data: {
                  userWalletId: wallet.id,

                  rechargePackId: data.rechargePackId,

                  paymentId: payment.id,

                  type: "CREDIT",

                  coins: totalCoins,

                  amount: data.amount,

                  description: "Recharge successful",
                },
              });

              /**
               * =================================================
               * CASHBACK
               * =================================================
               */

              if (
                data.couponType === "CASHBACK" &&
                Number(data.cashback || 0) > 0
              ) {
                const cashbackExists = await tx.walletTransaction.findFirst({
                  where: {
                    paymentId: payment.id,

                    type: "CASHBACK",
                  },
                });

                if (!cashbackExists) {
                  const cashbackAmount = Number(data.cashback || 0);

                  /**
                   * Credit cashback
                   */

                  await tx.userWallet.update({
                    where: {
                      id: wallet.id,
                    },

                    data: {
                      balanceCoins: {
                        increment: cashbackAmount,
                      },
                    },
                  });

                  /**
                   * Cashback transaction
                   */

                  await tx.walletTransaction.create({
                    data: {
                      userWalletId: wallet.id,

                      paymentId: payment.id,

                      rechargePackId: data.rechargePackId,

                      type: "CASHBACK",

                      coins: cashbackAmount,

                      amount: 0,

                      description: `Cashback (${data.couponCode || "Coupon"})`,
                    },
                  });

                  console.log(
                    `CASHBACK CREDITED: amount=${cashbackAmount}, coupon=${data.couponCode}`,
                  );
                }
              }

              /**
               * =================================================
               * DISCOUNT
               * =================================================
               */

              if (
                data.couponType === "DISCOUNT" &&
                Number(data.discount || 0) > 0
              ) {
                const discountExists = await tx.walletTransaction.findFirst({
                  where: {
                    paymentId: payment.id,

                    type: "DISCOUNT",
                  },
                });

                if (!discountExists) {
                  const discountAmount = Number(data.discount || 0);

                  await tx.walletTransaction.create({
                    data: {
                      userWalletId: wallet.id,

                      paymentId: payment.id,

                      rechargePackId: data.rechargePackId,

                      type: "DISCOUNT",

                      coins: discountAmount,

                      amount: 0,

                      description: `Discount Applied (${data.couponCode || "Coupon"})`,
                    },
                  });

                  console.log(
                    `DISCOUNT APPLIED: amount=${discountAmount}, coupon=${data.couponCode}`,
                  );
                }
              }

              /**
               * =================================================
               * SUCCESS LOG
               * =================================================
               */

              console.log(
                `SUCCESS: user=${data.userId}, coins=${data.coins}, payment=${data.paymentId}, coupon=${data.couponCode || "NONE"}`,
              );
            },
            {
              timeout: 10000,
            },
          );

          /**
           * ============================
           * ACK MESSAGE
           * ============================
           */

          channel.ack(msg);
        } catch (err) {
          console.error("Processing failed:", err);

          /**
           * ============================
           * REJECT MESSAGE
           * ============================
           *
           * Currently no requeue.
           */

          channel.nack(msg, false, false);
        }
      },
      {
        noAck: false,
      },
    );
  } catch (err) {
    console.error("Consumer startup failed:", err.message);

    /**
     * Retry connection after 5 seconds
     */

    setTimeout(startConsumer, 5000);
  }
}

/**
 * ============================
 * GRACEFUL SHUTDOWN
 * ============================
 */

async function shutdown() {
  console.log("Shutting down consumer...");

  try {
    if (channel) {
      await channel.close();
    }

    if (connection) {
      await connection.close();
    }

    await prisma.$disconnect();

    await pool.end();
  } catch (err) {
    console.error("Shutdown error:", err);
  }

  process.exit(0);
}

/**
 * ============================
 * PROCESS SIGNALS
 * ============================
 */

process.on("SIGINT", shutdown);

process.on("SIGTERM", shutdown);

/**
 * ============================
 * START CONSUMER
 * ============================
 */

startConsumer();
