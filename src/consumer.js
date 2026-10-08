// src/consumer.js

import "dotenv/config";
import amqp from "amqplib";

import pkg from "@prisma/client";
const { PrismaClient } = pkg;

import pg from "pg";
const { Pool } = pg;

import { PrismaPg } from "@prisma/adapter-pg";

/**
 * ============================================================
 * POSTGRESQL POOL
 * ============================================================
 */

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 10,
});

/**
 * ============================================================
 * PRISMA ADAPTER
 * ============================================================
 */

const adapter = new PrismaPg(pool);

/**
 * ============================================================
 * PRISMA CLIENT
 * ============================================================
 */

const prisma = new PrismaClient({
  adapter,
  log: ["error", "warn"],
});

let connection;
let channel;

const QUEUE_NAME = "payment.success";

/**
 * ============================================================
 * CONFIGURATION
 * ============================================================
 */

const GST_RATE = Number(
  process.env.GST_RATE || 18,
);

const COMPANY_STATE =
  process.env.COMPANY_STATE || "Delhi";

const PG_RATE = Number(
  process.env.PG_RATE || 1.65,
);

const PG_GST_RATE = Number(
  process.env.PG_GST_RATE || 18,
);

/**
 * ============================================================
 * CALCULATE GST
 * ============================================================
 *
 * Amount received from Razorpay is GST inclusive.
 */

function calculateGST(
  totalAmount,
  customerState,
) {
  console.log(
    "GST calculation totalAmount:",
    totalAmount,
  );

  const numericTotalAmount = Number(
    totalAmount || 0,
  );

  const taxableAmount = Number(
    (
      (numericTotalAmount * 100) /
      (100 + GST_RATE)
    ).toFixed(2),
  );

  const totalTax = Number(
    (
      numericTotalAmount -
      taxableAmount
    ).toFixed(2),
  );

  console.log(
    "GST taxableAmount:",
    taxableAmount,
  );

  console.log(
    "GST totalTax:",
    totalTax,
  );

  let cgst = 0;
  let sgst = 0;
  let igst = 0;

  if (
    customerState &&
    customerState.trim().toLowerCase() ===
      COMPANY_STATE.trim().toLowerCase()
  ) {
    cgst = Number(
      (totalTax / 2).toFixed(2),
    );

    sgst = Number(
      (totalTax / 2).toFixed(2),
    );
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
 * ============================================================
 * CALCULATE PAYMENT GATEWAY CHARGES
 * ============================================================
 */

function calculatePGCharges(totalAmount) {
  const numericTotalAmount = Number(
    totalAmount || 0,
  );

  const pgCharge = Number(
    (
      numericTotalAmount *
      (PG_RATE / 100)
    ).toFixed(2),
  );

  const pgIgst = Number(
    (
      pgCharge *
      (PG_GST_RATE / 100)
    ).toFixed(2),
  );

  const pgTotal = Number(
    (pgCharge + pgIgst).toFixed(2),
  );

  const receivableAmount = Number(
    (
      numericTotalAmount -
      pgTotal
    ).toFixed(2),
  );

  return {
    pgChargeRate: PG_RATE,
    pgCharge,
    pgIgst,
    pgTotal,
    receivableAmount,
  };
}

/**
 * ============================================================
 * SERVICE COUPON REDEMPTION
 * ============================================================
 *
 * SERVICE:
 * CouponRedemption.servicePaymentOrderId
 *
 * RECHARGE:
 * CouponRedemption.paymentOrderId
 */

async function redeemServiceCoupon(
  tx,
  data,
  servicePaymentOrder,
) {
  console.log(
    "Service coupon redemption data:",
    data,
  );

  if (
    !data.couponCode ||
    data.couponCode.trim() === ""
  ) {
    return;
  }

  console.log(
    "Processing SERVICE coupon redemption:",
    {
      couponCode: data.couponCode,
      couponType: data.couponType,
      userId: data.userId,
      discount: data.discount,
      cashback: data.cashback,
    },
  );

  /**
   * ============================================================
   * FIND COUPON
   * ============================================================
   */

  const coupon =
    await tx.coupon.findUnique({
      where: {
        code: data.couponCode
          .trim()
          .toUpperCase(),
      },
    });

  if (!coupon) {
    throw new Error(
      `Coupon not found: ${data.couponCode}`,
    );
  }

  /**
   * ============================================================
   * CHECK USER REDEMPTION
   * ============================================================
   */

  const existingRedemption =
    await tx.couponRedemption.findFirst({
      where: {
        couponId: coupon.id,
        userId: data.userId,
      },
    });

  if (existingRedemption) {
    console.log(
      `SERVICE COUPON ALREADY REDEEMED: coupon=${coupon.code}, user=${data.userId}`,
    );

    return;
  }

  /**
   * ============================================================
   * CHECK GLOBAL COUPON LIMIT
   * ============================================================
   */

  if (
    coupon.redeemLimit !== null &&
    coupon.redeemLimit !== undefined &&
    Number(coupon.usedCount || 0) >=
      Number(coupon.redeemLimit)
  ) {
    throw new Error(
      `Coupon ${coupon.code} redemption limit has been reached`,
    );
  }

  /**
   * ============================================================
   * CREATE SERVICE REDEMPTION
   * ============================================================
   */

  await tx.couponRedemption.create({
    data: {
      couponId: coupon.id,

      userId: data.userId,

      servicePaymentOrderId:
        servicePaymentOrder.id,

      discount: Number(
        data.discount || 0,
      ),
    },
  });

  /**
   * ============================================================
   * INCREMENT COUPON USAGE
   * ============================================================
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
 * ============================================================
 * RECHARGE COUPON REDEMPTION
 * ============================================================
 *
 * Recharge coupon is stored in:
 *
 * PaymentOrder.couponId
 */

async function redeemRechargeCoupon(
  tx,
  data,
  paymentOrder,
) {
  /**
   * No coupon applied
   */

  if (!paymentOrder.couponId) {
    return;
  }

  console.log(
    "Processing RECHARGE coupon redemption:",
    {
      couponId: paymentOrder.couponId,
      couponCode: data.couponCode,
      couponType: data.couponType,
      userId: data.userId,
      discount: data.discount,
      cashback: data.cashback,
    },
  );

  /**
   * ============================================================
   * FIND COUPON
   * ============================================================
   */

  const coupon =
    await tx.coupon.findUnique({
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
   * ============================================================
   * CHECK USER REDEMPTION
   * ============================================================
   */

  const existingRedemption =
    await tx.couponRedemption.findFirst({
      where: {
        couponId: coupon.id,
        userId: data.userId,
      },
    });

  if (existingRedemption) {
    console.log(
      `RECHARGE COUPON ALREADY REDEEMED: coupon=${coupon.code}, couponId=${coupon.id}, user=${data.userId}`,
    );

    return;
  }

  /**
   * ============================================================
   * CHECK GLOBAL LIMIT
   * ============================================================
   */

  if (
    coupon.redeemLimit !== null &&
    coupon.redeemLimit !== undefined &&
    Number(coupon.usedCount || 0) >=
      Number(coupon.redeemLimit)
  ) {
    throw new Error(
      `Coupon ${coupon.code} redemption limit has been reached`,
    );
  }

  /**
   * ============================================================
   * CREATE RECHARGE REDEMPTION
   * ============================================================
   */

  await tx.couponRedemption.create({
    data: {
      couponId: paymentOrder.couponId,

      userId: data.userId,

      paymentOrderId:
        paymentOrder.id,

      discount: Number(
        data.discount || 0,
      ),
    },
  });

  /**
   * ============================================================
   * INCREMENT COUPON USAGE
   * ============================================================
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
 * ============================================================
 * CREDIT CASHBACK TO USER WALLET
 * ============================================================
 *
 * Used by:
 *
 * 1. RECHARGE CASHBACK
 * 2. SERVICE CASHBACK
 *
 * Recharge:
 *
 * paymentId -> WalletTransaction.paymentId
 *
 * Service:
 *
 * servicePaymentOrderId ->
 * WalletTransaction.servicePaymentOrderId
 */

async function creditCashback(
  tx,
  {
    userId,
    cashbackAmount,
    paymentId = null,
    servicePaymentOrderId = null,
    rechargePackId = null,
    couponCode = null,
  },
) {
  const amount = Number(
    cashbackAmount || 0,
  );

  /**
   * ============================================================
   * NO CASHBACK
   * ============================================================
   */

  if (amount <= 0) {
    console.log(
      `No cashback to credit: user=${userId}, amount=${amount}`,
    );

    return;
  }

  console.log(
    "Processing cashback:",
    {
      userId,
      amount,
      paymentId,
      servicePaymentOrderId,
      rechargePackId,
      couponCode,
    },
  );

  /**
   * ============================================================
   * CHECK DUPLICATE CASHBACK
   * ============================================================
   */

  let cashbackExists = null;

  /**
   * RECHARGE
   */

  if (paymentId) {
    cashbackExists =
      await tx.walletTransaction.findFirst({
        where: {
          paymentId,
          type: "CASHBACK",
        },
      });
  }

  /**
   * SERVICE
   */

  if (
    !cashbackExists &&
    servicePaymentOrderId
  ) {
    cashbackExists =
      await tx.walletTransaction.findFirst({
        where: {
          servicePaymentOrderId,
          type: "CASHBACK",
        },
      });
  }

  if (cashbackExists) {
    console.log(
      `CASHBACK ALREADY CREDITED: user=${userId}, amount=${amount}`,
    );

    return;
  }

  /**
   * ============================================================
   * GET / CREATE USER WALLET
   * ============================================================
   */

  const wallet =
    await tx.userWallet.upsert({
      where: {
        userId,
      },

      update: {},

      create: {
        userId,

        balanceCoins: 0,

        lockedCoins: 0,
      },
    });

  /**
   * ============================================================
   * CREDIT CASHBACK
   * ============================================================
   */

  await tx.userWallet.update({
    where: {
      id: wallet.id,
    },

    data: {
      balanceCoins: {
        increment: amount,
      },
    },
  });

  /**
   * ============================================================
   * CREATE CASHBACK TRANSACTION
   * ============================================================
   */

  await tx.walletTransaction.create({
    data: {
      userWalletId: wallet.id,

      paymentId,

      servicePaymentOrderId,

      rechargePackId,

      type: "CASHBACK",

      coins: amount,

      amount: 0,

      description:
        `Cashback (${couponCode || "Coupon"})`,
    },
  });

  console.log(
    `CASHBACK CREDITED SUCCESSFULLY: user=${userId}, amount=${amount}, coupon=${couponCode || "NONE"}, paymentId=${paymentId || "NONE"}, servicePaymentOrderId=${servicePaymentOrderId || "NONE"}`,
  );
}

/**
 * ============================================================
 * START CONSUMER
 * ============================================================
 */

async function startConsumer() {
  try {
    console.log(
      "Connecting to RabbitMQ...",
    );

    connection = await amqp.connect(
      process.env.RABBITMQ_URL,
    );

    connection.on(
      "error",
      (err) => {
        console.error(
          "RabbitMQ connection error:",
          err.message,
        );
      },
    );

    connection.on(
      "close",
      () => {
        console.warn(
          "RabbitMQ connection closed. Reconnecting...",
        );

        setTimeout(
          startConsumer,
          5000,
        );
      },
    );

    channel =
      await connection.createChannel();

    /**
     * ============================================================
     * ENSURE QUEUE EXISTS
     * ============================================================
     */

    await channel.assertQueue(
      QUEUE_NAME,
      {
        durable: true,
      },
    );

    /**
     * ============================================================
     * PREVENT PARALLEL PROCESSING
     * ============================================================
     */

    channel.prefetch(1);

    console.log(
      `Waiting for messages in queue: ${QUEUE_NAME}`,
    );

    /**
     * ============================================================
     * CONSUME PAYMENT MESSAGE
     * ============================================================
     */

    channel.consume(
      QUEUE_NAME,

      async (msg) => {
        if (!msg) {
          return;
        }

        let data;

        /**
         * ============================================================
         * PARSE MESSAGE
         * ============================================================
         */

        try {
          data = JSON.parse(
            msg.content.toString(),
          );
        } catch (err) {
          console.error(
            "Invalid JSON message",
            err,
          );

          channel.nack(
            msg,
            false,
            false,
          );

          return;
        }

        console.log(
          "Received payment.success message:",
          data,
        );

        try {
          /**
           * ============================================================
           * PRISMA TRANSACTION
           * ============================================================
           */

          await prisma.$transaction(
            async (tx) => {
              /**
               * ========================================================
               * SERVICE PAYMENT
               * ========================================================
               */

              if (
                data.serviceType ===
                "SERVICE"
              ) {
                /**
                 * ======================================================
                 * FIND SERVICE PAYMENT ORDER
                 * ======================================================
                 */

                const servicePaymentOrder =
                  await tx.servicePaymentOrder.findUnique(
                    {
                      where: {
                        razorpayOrderId:
                          data.orderId,
                      },
                    },
                  );

                if (
                  !servicePaymentOrder
                ) {
                  throw new Error(
                    `Service payment order not found: ${data.orderId}`,
                  );
                }

                /**
                 * ======================================================
                 * FAILED SERVICE PAYMENT
                 * ======================================================
                 */

                if (
                  data.status ===
                  "failed"
                ) {
                  await tx.servicePaymentOrder.update(
                    {
                      where: {
                        id: servicePaymentOrder.id,
                      },

                      data: {
                        status: "FAILED",
                      },
                    },
                  );

                  console.log(
                    `SERVICE PAYMENT FAILED: order=${data.orderId}`,
                  );

                  return;
                }

                /**
                 * ======================================================
                 * ONLY PROCESS CAPTURED PAYMENT
                 * ======================================================
                 */

                if (
                  data.status !==
                  "captured"
                ) {
                  console.log(
                    `Ignoring unsupported service payment status: ${data.status}`,
                  );

                  return;
                }

                /**
                 * ======================================================
                 * PREVENT DUPLICATE SERVICE PAYMENT
                 * ======================================================
                 */

                if (
                  servicePaymentOrder.status ===
                  "PAID"
                ) {
                  console.log(
                    `Service payment already processed: ${data.orderId}`,
                  );

                  return;
                }

                /**
                 * ======================================================
                 * FIND SERVICE COUPON
                 * ======================================================
                 */

                let couponId =
                  null;

                if (
                  data.couponCode &&
                  data.couponCode.trim() !==
                    ""
                ) {
                  const coupon =
                    await tx.coupon.findUnique(
                      {
                        where: {
                          code: data.couponCode
                            .trim()
                            .toUpperCase(),
                        },

                        select: {
                          id: true,
                          code: true,
                          type: true,
                        },
                      },
                    );

                  if (!coupon) {
                    throw new Error(
                      `Coupon not found: ${data.couponCode}`,
                    );
                  }

                  couponId = coupon.id;

                  console.log(
                    "SERVICE COUPON FOUND:",
                    {
                      couponId:
                        coupon.id,

                      couponCode:
                        coupon.code,

                      couponType:
                        coupon.type,
                    },
                  );
                }

                /**
                 * ======================================================
                 * NORMALIZE SERVICE PAYMENT VALUES
                 * ======================================================
                 */

                const totalAmount =
                  Number(
                    data.coins || 0,
                  );

                const payableAmount =
                  Number(
                    data.amount || 0,
                  );

                const discount =
                  Number(
                    data.discount || 0,
                  );

                const cashback =
                  Number(
                    data.cashback || 0,
                  );

                console.log(
                  "SERVICE PAYMENT VALUES:",
                  {
                    orderId:
                      data.orderId,

                    paymentId:
                      data.paymentId,

                    totalAmount,

                    payableAmount,

                    couponId,

                    discount,

                    cashback,
                  },
                );

                /**
                 * ======================================================
                 * UPDATE SERVICE PAYMENT ORDER
                 * ======================================================
                 */

                await tx.servicePaymentOrder.update(
                  {
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
                  },
                );

                console.log(
                  `SERVICE PAYMENT ORDER UPDATED: order=${data.orderId}`,
                );

                /**
                 * ======================================================
                 * UPDATE SERVICE BOOKING
                 * ======================================================
                 */

                await tx.serviceBooking.update(
                  {
                    where: {
                      id: servicePaymentOrder.bookingId,
                    },

                    data: {
                      paymentStatus:
                        "SUCCESS",

                      bookingStatus:
                        "ASSIGNED",
                    },
                  },
                );

                /**
                 * ======================================================
                 * SERVICE COUPON REDEMPTION
                 * ======================================================
                 */

                if (couponId) {
                  await redeemServiceCoupon(
                    tx,
                    data,
                    {
                      ...servicePaymentOrder,

                      couponId,
                    },
                  );
                }

                /**
                 * ======================================================
                 * SERVICE CASHBACK
                 * ======================================================
                 *
                 * IMPORTANT:
                 *
                 * Service cashback is actual wallet
                 * credit.
                 */

                if (
                  data.couponType ===
                    "CASHBACK" &&
                  cashback > 0
                ) {
                  await creditCashback(
                    tx,
                    {
                      userId:
                        data.userId,

                      cashbackAmount:
                        cashback,

                      paymentId: null,

                      servicePaymentOrderId:
                        servicePaymentOrder.id,

                      rechargePackId:
                        null,

                      couponCode:
                        data.couponCode ||
                        null,
                    },
                  );
                }

                /**
                 * ======================================================
                 * SERVICE DISCOUNT TRANSACTION
                 * ======================================================
                 *
                 * Discount is NOT added to wallet.
                 *
                 * It is only recorded against
                 * ServicePaymentOrder / CouponRedemption.
                 */

                /**
                 * ======================================================
                 * SERVICE SUCCESS
                 * ======================================================
                 */

                console.log(
                  `SERVICE PAYMENT SUCCESS: booking=${servicePaymentOrder.bookingId}, payment=${servicePaymentOrder.id}, totalAmount=${totalAmount}, payableAmount=${payableAmount}, coupon=${data.couponCode || "NONE"}, discount=${discount}, cashback=${cashback}`,
                );

                return;
              }

              /**
               * ========================================================
               * RECHARGE / NORMAL WALLET PAYMENT
               * ========================================================
               */

              const paymentOrder =
                await tx.paymentOrder.findUnique(
                  {
                    where: {
                      razorpayOrderId:
                        data.orderId,
                    },
                  },
                );

              if (!paymentOrder) {
                throw new Error(
                  `Payment order not found: ${data.orderId}`,
                );
              }

              /**
               * ======================================================
               * FAILED PAYMENT
               * ======================================================
               */

              if (
                data.status ===
                "failed"
              ) {
                await tx.paymentOrder.update(
                  {
                    where: {
                      id: paymentOrder.id,
                    },

                    data: {
                      status: "FAILED",
                    },
                  },
                );

                console.log(
                  `FAILED PAYMENT: order=${data.orderId}, payment=${data.paymentId}`,
                );

                return;
              }

              /**
               * ======================================================
               * ONLY HANDLE CAPTURED
               * ======================================================
               */

              if (
                data.status !==
                "captured"
              ) {
                console.log(
                  `Ignoring unsupported payment status: ${data.status}`,
                );

                return;
              }

              /**
               * ======================================================
               * PREVENT DUPLICATE PAYMENT PROCESSING
               * ======================================================
               */

              const existingPayment =
                await tx.payment.findUnique(
                  {
                    where: {
                      razorpayPaymentId:
                        data.paymentId,
                    },
                  },
                );

              if (existingPayment) {
                console.log(
                  `Payment already processed: ${data.paymentId}`,
                );

                return;
              }

              /**
               * ======================================================
               * UPDATE PAYMENT ORDER
               * ======================================================
               */

              await tx.paymentOrder.update(
                {
                  where: {
                    id: paymentOrder.id,
                  },

                  data: {
                    status: "PAID",
                  },
                },
              );

              /**
               * ======================================================
               * CALCULATE GST
               * ======================================================
               */

              const gst =
                calculateGST(
                  data.amount,
                  data.state,
                );

              /**
               * ======================================================
               * CALCULATE PG CHARGES
               * ======================================================
               */

              const pg =
                calculatePGCharges(
                  data.amount,
                );

              /**
               * ======================================================
               * GENERATE INVOICE NUMBER
               * ======================================================
               */

              const invoiceNo =
                `INV-${new Date().getFullYear()}-${Date.now()}`;

              console.log(
                "taxableAmount:",
                gst.taxableAmount,
              );

              /**
               * ======================================================
               * CREATE PAYMENT RECORD
               * ======================================================
               */

              const payment =
                await tx.payment.create({
                  data: {
                    userId:
                      data.userId,

                    rechargePackId:
                      data.rechargePackId,

                    paymentOrderId:
                      paymentOrder.id,

                    amount:
                      data.amount,

                    coins:
                      data.coins,

                    provider:
                      "RAZORPAY",

                    razorpayOrderId:
                      data.orderId,

                    razorpayPaymentId:
                      data.paymentId,

                    status:
                      "SUCCESS",

                    invoiceNo,

                    taxableAmount:
                      gst.taxableAmount,

                    gstRate:
                      gst.gstRate,

                    cgst:
                      gst.cgst,

                    sgst:
                      gst.sgst,

                    igst:
                      gst.igst,

                    totalTax:
                      gst.totalTax,

                    totalAmount:
                      gst.totalAmount,

                    pgChargeRate:
                      pg.pgChargeRate,

                    pgCharge:
                      pg.pgCharge,

                    pgIgst:
                      pg.pgIgst,

                    pgTotal:
                      pg.pgTotal,

                    receivableAmount:
                      pg.receivableAmount,

                    country:
                      data.country,

                    state:
                      data.state,

                    city:
                      data.city,

                    platform:
                      data.platform,
                  },
                });

              /**
               * ======================================================
               * RECHARGE COUPON REDEMPTION
               * ======================================================
               */

              await redeemRechargeCoupon(
                tx,
                data,
                paymentOrder,
              );

              /**
               * ======================================================
               * CHECK DUPLICATE RECHARGE CREDIT
               * ======================================================
               *
               * IMPORTANT:
               *
               * Do this BEFORE incrementing wallet balance.
               */

              const existingWalletTx =
                await tx.walletTransaction.findFirst(
                  {
                    where: {
                      paymentId:
                        payment.id,

                      type:
                        "CREDIT",
                    },
                  },
                );

              if (existingWalletTx) {
                console.log(
                  `Wallet credit already exists for payment=${payment.id}`,
                );

                return;
              }

              /**
               * ======================================================
               * RECHARGE WALLET CREDIT
               * ======================================================
               */

              const totalCoins =
                Number(
                  data.coins || 0,
                );

              const wallet =
                await tx.userWallet.upsert(
                  {
                    where: {
                      userId:
                        data.userId,
                    },

                    update: {
                      balanceCoins: {
                        increment:
                          totalCoins,
                      },
                    },

                    create: {
                      userId:
                        data.userId,

                      balanceCoins:
                        totalCoins,

                      lockedCoins: 0,
                    },
                  },
                );

              /**
               * ======================================================
               * CREATE RECHARGE CREDIT TRANSACTION
               * ======================================================
               */

              await tx.walletTransaction.create(
                {
                  data: {
                    userWalletId:
                      wallet.id,

                    rechargePackId:
                      data.rechargePackId,

                    paymentId:
                      payment.id,

                    type:
                      "CREDIT",

                    coins:
                      totalCoins,

                    amount:
                      data.amount,

                    description:
                      "Recharge successful",
                  },
                },
              );

              /**
               * ======================================================
               * RECHARGE CASHBACK
               * ======================================================
               */

              if (
                data.couponType ===
                  "CASHBACK" &&
                Number(
                  data.cashback || 0,
                ) > 0
              ) {
                await creditCashback(
                  tx,
                  {
                    userId:
                      data.userId,

                    cashbackAmount:
                      Number(
                        data.cashback ||
                          0,
                      ),

                    paymentId:
                      payment.id,

                    servicePaymentOrderId:
                      null,

                    rechargePackId:
                      data.rechargePackId,

                    couponCode:
                      data.couponCode ||
                      null,
                  },
                );
              }

              /**
               * ======================================================
               * RECHARGE DISCOUNT
               * ======================================================
               *
               * Discount does NOT increase wallet balance.
               *
               * It is only recorded as a transaction.
               */

              if (
                data.couponType ===
                  "DISCOUNT" &&
                Number(
                  data.discount || 0,
                ) > 0
              ) {
                const discountExists =
                  await tx.walletTransaction.findFirst(
                    {
                      where: {
                        paymentId:
                          payment.id,

                        type:
                          "DISCOUNT",
                      },
                    },
                  );

                if (!discountExists) {
                  const discountAmount =
                    Number(
                      data.discount ||
                        0,
                    );

                  await tx.walletTransaction.create(
                    {
                      data: {
                        userWalletId:
                          wallet.id,

                        paymentId:
                          payment.id,

                        rechargePackId:
                          data.rechargePackId,

                        type:
                          "DISCOUNT",

                        coins:
                          discountAmount,

                        amount: 0,

                        description:
                          `Discount Applied (${data.couponCode || "Coupon"})`,
                      },
                    },
                  );

                  console.log(
                    `DISCOUNT APPLIED: amount=${discountAmount}, coupon=${data.couponCode}`,
                  );
                }
              }

              /**
               * ======================================================
               * SUCCESS LOG
               * ======================================================
               */

              console.log(
                `SUCCESS: user=${data.userId}, coins=${data.coins}, payment=${data.paymentId}, coupon=${data.couponCode || "NONE"}, couponType=${data.couponType || "NONE"}, discount=${data.discount || 0}, cashback=${data.cashback || 0}`,
              );
            },

            {
              timeout: 10000,
            },
          );

          /**
           * ============================================================
           * ACK MESSAGE
           * ============================================================
           */

          channel.ack(msg);
        } catch (err) {
          console.error(
            "Processing failed:",
            err,
          );

          /**
           * ============================================================
           * REJECT MESSAGE
           * ============================================================
           *
           * Currently no requeue.
           */

          channel.nack(
            msg,
            false,
            false,
          );
        }
      },

      {
        noAck: false,
      },
    );
  } catch (err) {
    console.error(
      "Consumer startup failed:",
      err.message,
    );

    /**
     * Retry after 5 seconds
     */

    setTimeout(
      startConsumer,
      5000,
    );
  }
}

/**
 * ============================================================
 * GRACEFUL SHUTDOWN
 * ============================================================
 */

async function shutdown() {
  console.log(
    "Shutting down consumer...",
  );

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
    console.error(
      "Shutdown error:",
      err,
    );
  }

  process.exit(0);
}

/**
 * ============================================================
 * PROCESS SIGNALS
 * ============================================================
 */

process.on(
  "SIGINT",
  shutdown,
);

process.on(
  "SIGTERM",
  shutdown,
);

/**
 * ============================================================
 * START CONSUMER
 * ============================================================
 */

startConsumer();