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

async function redeemPujaCoupon(tx, data, pujaOrder) {
  const couponId = pujaOrder.couponId;
  const userId = pujaOrder.userId;
  const pujaPaymentOrderId = pujaOrder.id;

  if (!couponId) {
    console.log("No coupon applied to PUJA order:", pujaOrder.razorpayOrderId);
    return;
  }

  // Avoid creating the same redemption twice.
  const existingRedemption = await tx.couponRedemption.findFirst({
    where: {
      couponId,
      userId,
      pujaPaymentOrderId,
    },
  });

  if (existingRedemption) {
    console.log("PUJA coupon redemption already exists:", {
      pujaPaymentOrderId,
      couponId,
      userId,
    });
    return;
  }

  const coupon = await tx.coupon.findUnique({
    where: {
      id: couponId,
    },
  });

  if (!coupon) {
    throw new Error(`Coupon not found: ${couponId}`);
  }

  // Preserve the existing coupon eligibility/limit checks used by your
  // service helper, including your intended per-user redemption rules.
  const existingUserRedemption = await tx.couponRedemption.findFirst({
    where: {
      couponId,
      userId,
    },
  });

  if (existingUserRedemption) {
    throw new Error(
      `User ${userId} has already redeemed coupon ${coupon.code}`
    );
  }

  if (
    coupon.redeemLimit != null &&
    coupon.usedCount >= coupon.redeemLimit
  ) {
    throw new Error(`Coupon redemption limit reached: ${coupon.code}`);
  }

  const discount = Number(data.discount ?? pujaOrder.discount ?? 0);

  await tx.couponRedemption.create({
    data: {
      couponId,
      userId,
      pujaPaymentOrderId,
      discount,
    },
  });

  await tx.coupon.update({
    where: {
      id: couponId,
    },
    data: {
      usedCount: {
        increment: 1,
      },
    },
  });

  console.log("PUJA coupon redemption created:", {
    pujaPaymentOrderId,
    couponId,
    couponCode: coupon.code,
    userId,
    discount,
  });
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

              /**
               * =================================================
               * SERVICE PAYMENT
               * =================================================
               *
               * Handles:
               * 1. Service payment verification
               * 2. ServicePaymentOrder update
               * 3. User wallet deduction
               * 4. Wallet DEBIT transaction
               * 5. Coupon redemption
               * 6. Cashback credit
               * 7. Wallet CASHBACK transaction
               * 8. ServiceBooking status update
               *
               * All operations run inside the existing Prisma
               * transaction.
               */

              if (data.serviceType === "SERVICE") {
                console.log("Processing SERVICE payment:", data.orderId);

                /**
                 * =================================================
                 * VALIDATE ORDER ID
                 * =================================================
                 */

                const orderId = data.orderId;

                if (!orderId) {
                  throw new Error("Missing SERVICE Razorpay order ID");
                }

                /**
                 * =================================================
                 * FIND SERVICE PAYMENT ORDER
                 * =================================================
                 */

                const servicePaymentOrder =
                  await tx.servicePaymentOrder.findUnique({
                    where: {
                      razorpayOrderId: orderId,
                    },
                  });

                if (!servicePaymentOrder) {
                  throw new Error(
                    `Service payment order not found: ${orderId}`,
                  );
                }

                /**
                 * =================================================
                 * HANDLE FAILED PAYMENT
                 * =================================================
                 */

                if (data.status === "failed") {
                  if (servicePaymentOrder.status !== "PAID") {
                    await tx.servicePaymentOrder.update({
                      where: {
                        id: servicePaymentOrder.id,
                      },
                      data: {
                        status: "FAILED",
                      },
                    });
                  }

                  console.log("SERVICE payment failed:", orderId);

                  return;
                }

                /**
                 * =================================================
                 * PROCESS ONLY CAPTURED PAYMENTS
                 * =================================================
                 */

                if (data.status !== "captured") {
                  console.log("Ignoring SERVICE payment status:", data.status);

                  return;
                }

                /**
                 * =================================================
                 * PREVENT DUPLICATE PROCESSING
                 * =================================================
                 */

                if (servicePaymentOrder.status === "PAID") {
                  console.log("SERVICE payment already processed:", orderId);

                  return;
                }

                /**
                 * =================================================
                 * VALIDATE USER AND BOOKING
                 * =================================================
                 */

                const userId = servicePaymentOrder.userId;

                if (!userId) {
                  throw new Error(
                    `User ID missing for SERVICE order ${orderId}`,
                  );
                }

                if (!servicePaymentOrder.bookingId) {
                  throw new Error(
                    `Booking ID missing for SERVICE order ${orderId}`,
                  );
                }

                /**
                 * =================================================
                 * FIND COUPON
                 * =================================================
                 *
                 * couponId stored in ServicePaymentOrder must
                 * contain Coupon.id, not Coupon.code.
                 */

                let coupon = null;

                let couponId = servicePaymentOrder.couponId ?? null;

                if (couponId) {
                  coupon = await tx.coupon.findUnique({
                    where: {
                      id: couponId,
                    },
                    select: {
                      id: true,
                      code: true,
                      type: true,
                    },
                  });

                  if (!coupon) {
                    throw new Error(`SERVICE coupon not found: ${couponId}`);
                  }
                } else if (
                  typeof data.couponCode === "string" &&
                  data.couponCode.trim() !== ""
                ) {
                  coupon = await tx.coupon.findUnique({
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
                    throw new Error(
                      `SERVICE coupon not found: ${data.couponCode}`,
                    );
                  }

                  couponId = coupon.id;
                }

                /**
                 * =================================================
                 * NORMALIZE AMOUNTS
                 * =================================================
                 *
                 * totalAmount:
                 *   Total service coins/amount recorded for the order.
                 *
                 * payableAmount:
                 *   Amount captured through Razorpay.
                 *
                 * walletAmount:
                 *   Amount of the service cost funded from the wallet.
                 *
                 * discount:
                 *   Coupon discount.
                 *
                 * cashback:
                 *   Cashback coins awarded to the user.
                 *
                 * IMPORTANT:
                 * Do not deduct payableAmount from the wallet.
                 * Razorpay has already captured that amount.
                 *
                 * Deduct walletAmount only when the order records
                 * an actual wallet contribution.
                 */

                const totalAmount = Number(
                  data.coins ?? servicePaymentOrder.totalAmount ?? 0,
                );

                const payableAmount = Number(
                  data.amount ?? servicePaymentOrder.payableAmount ?? 0,
                );

                const walletAmount = Number(
                  servicePaymentOrder.walletAmount ?? 0,
                );

                const discount = Number(
                  data.discount ?? servicePaymentOrder.discount ?? 0,
                );

                const cashback = Number(
                  data.cashback ?? servicePaymentOrder.cashback ?? 0,
                );

                const couponCode = coupon?.code ?? null;

                const couponType = String(
                  coupon?.type ?? data.couponType ?? "",
                ).toUpperCase();

                /**
                 * =================================================
                 * VALIDATE PAYMENT AMOUNTS
                 * =================================================
                 */

                const amountValues = {
                  totalAmount,
                  payableAmount,
                  walletAmount,
                  discount,
                  cashback,
                };

                for (const [key, value] of Object.entries(amountValues)) {
                  if (!Number.isFinite(value) || value < 0) {
                    throw new Error(
                      `Invalid SERVICE payment amount: ${key}=${value}, order=${orderId}`,
                    );
                  }
                }

                /**
                 * =================================================
                 * DETERMINE CASHBACK ELIGIBILITY
                 * =================================================
                 */

                const cashbackAmount =
                  couponId && couponType === "CASHBACK" && cashback > 0
                    ? cashback
                    : 0;

                console.log("SERVICE PAYMENT DETAILS:", {
                  orderId,
                  userId,
                  bookingId: servicePaymentOrder.bookingId,
                  totalAmount,
                  payableAmount,
                  walletAmount,
                  discount,
                  cashback,
                  cashbackAmount,
                  couponId,
                  couponCode,
                  couponType,
                });

                /**
                 * =================================================
                 * CLAIM SERVICE PAYMENT ORDER
                 * =================================================
                 *
                 * Conditional update protects against concurrent
                 * consumers attempting to process the same order.
                 *
                 * Because this update is inside the transaction,
                 * any later failure rolls it back.
                 */

                const claimedOrder = await tx.servicePaymentOrder.updateMany({
                  where: {
                    id: servicePaymentOrder.id,
                    status: {
                      not: "PAID",
                    },
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

                if (claimedOrder.count === 0) {
                  console.log("SERVICE payment already claimed:", orderId);

                  return;
                }

                /**
                 * =================================================
                 * GET OR CREATE USER WALLET
                 * =================================================
                 */

                let wallet = null;

                let previousBalance = 0;

                let balanceAfterDeduction = 0;

                let finalBalance = 0;

                if (walletAmount > 0 || cashbackAmount > 0) {
                  wallet = await tx.userWallet.upsert({
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

                  previousBalance = Number(wallet.balanceCoins);

                  balanceAfterDeduction = previousBalance;

                  finalBalance = previousBalance;

                  console.log("SERVICE USER WALLET:", {
                    userId,
                    walletId: wallet.id,
                    previousBalance,
                  });
                }

                /**
                 * =================================================
                 * DEDUCT SERVICE COST FROM USER WALLET
                 * =================================================
                 *
                 * Only walletAmount is deducted.
                 *
                 * payableAmount is the Razorpay-captured amount
                 * and must not be deducted a second time.
                 */

                if (wallet && walletAmount > 0) {
                  const deduction = await tx.userWallet.updateMany({
                    where: {
                      id: wallet.id,
                      userId,
                      balanceCoins: {
                        gte: walletAmount,
                      },
                    },
                    data: {
                      balanceCoins: {
                        decrement: walletAmount,
                      },
                    },
                  });

                  if (deduction.count === 0) {
                    throw new Error(
                      `Insufficient wallet balance for SERVICE order ${orderId}. ` +
                        `Available: ${previousBalance}, required: ${walletAmount}`,
                    );
                  }

                  /**
                   * =================================================
                   * CREATE WALLET DEBIT TRANSACTION
                   * =================================================
                   */

                  await tx.walletTransaction.create({
                    data: {
                      userWalletId: wallet.id,
                      type: "DEBIT",
                      coins: walletAmount,
                      amount: 0,
                      description: `Service payment (${orderId})`,
                    },
                  });

                  /**
                   * =================================================
                   * READ BALANCE AFTER DEDUCTION
                   * =================================================
                   */

                  const updatedWallet = await tx.userWallet.findUnique({
                    where: {
                      id: wallet.id,
                    },
                  });

                  if (!updatedWallet) {
                    throw new Error(
                      `Wallet not found after SERVICE deduction: ${userId}`,
                    );
                  }

                  balanceAfterDeduction = Number(updatedWallet.balanceCoins);

                  finalBalance = balanceAfterDeduction;

                  console.log("SERVICE WALLET DEDUCTED:", {
                    orderId,
                    userId,
                    walletAmount,
                    previousBalance,
                    balanceAfterDeduction,
                  });
                }

                /**
                 * =================================================
                 * CREDIT CASHBACK TO USER WALLET
                 * =================================================
                 *
                 * Cashback is credited only for CASHBACK coupons.
                 *
                 * DISCOUNT coupons do not receive cashback from
                 * this block.
                 */

                if (wallet && cashbackAmount > 0) {
                  const updatedWallet = await tx.userWallet.update({
                    where: {
                      id: wallet.id,
                    },
                    data: {
                      balanceCoins: {
                        increment: cashbackAmount,
                      },
                    },
                  });

                  finalBalance = Number(updatedWallet.balanceCoins);

                  /**
                   * =================================================
                   * CREATE CASHBACK TRANSACTION
                   * =================================================
                   */

                  await tx.walletTransaction.create({
                    data: {
                      userWalletId: wallet.id,
                      type: "CASHBACK",
                      coins: cashbackAmount,
                      amount: 0,
                      description: `Service cashback (${couponCode ?? couponId})`,
                    },
                  });

                  console.log("SERVICE CASHBACK CREDITED:", {
                    orderId,
                    userId,
                    couponCode,
                    cashbackAmount,
                    balanceAfterDeduction,
                    finalBalance,
                  });
                } else {
                  console.log("SERVICE cashback not applicable:", {
                    orderId,
                    couponCode,
                    couponType,
                    cashback,
                  });
                }

                if (couponId && couponType === "DISCOUNT" && discount > 0) {
                  /**
                   * Get or create wallet for recording the transaction.
                   *
                   * A wallet is needed even when no wallet coins
                   * were used to pay for the service.
                   */

                  if (!wallet) {
                    wallet = await tx.userWallet.upsert({
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

                    previousBalance = Number(wallet.balanceCoins);

                    balanceAfterDeduction = previousBalance;

                    finalBalance = previousBalance;
                  }

                  /**
                   * Prevent duplicate discount transactions.
                   *
                   * Your WalletTransaction schema does not currently
                   * show a servicePaymentOrderId field, so use the
                   * unique order ID in the description for lookup.
                   */

                  const discountDescription = `Service discount (${couponCode || couponId}) - Order ${orderId}`;

                  const existingDiscountTransaction =
                    await tx.walletTransaction.findFirst({
                      where: {
                        userWalletId: wallet.id,
                        type: "DISCOUNT",
                        description: discountDescription,
                      },
                    });

                  if (!existingDiscountTransaction) {
                    /**
                     * Create discount transaction.
                     *
                     * IMPORTANT:
                     * Do not update balanceCoins here because the
                     * discount is not a wallet credit.
                     */

                    await tx.walletTransaction.create({
                      data: {
                        userWalletId: wallet.id,
                        type: "DISCOUNT",
                        coins: discount,
                        amount: 0,
                        description: discountDescription,
                      },
                    });

                    console.log("SERVICE DISCOUNT RECORDED:", {
                      orderId,
                      userId,
                      walletId: wallet.id,
                      couponId,
                      couponCode,
                      discount,
                      walletBalance: finalBalance,
                    });
                  } else {
                    console.log(
                      "SERVICE discount transaction already exists:",
                      orderId,
                    );
                  }
                }

                /**
                 * =================================================
                 * REDEEM SERVICE COUPON
                 * =================================================
                 */

                if (couponId) {
                  /**
                   * redeemServiceCoupon expects couponCode in data.
                   *
                   * Preserve the event payload but ensure the
                   * normalized coupon code is available.
                   */

                  const couponRedemptionData = {
                    ...data,
                    userId,
                    couponCode,
                    couponType,
                    discount,
                    cashback,
                  };

                  await redeemServiceCoupon(tx, couponRedemptionData, {
                    ...servicePaymentOrder,
                    id: servicePaymentOrder.id,
                    couponId,
                  });

                  console.log("SERVICE COUPON REDEEMED:", {
                    orderId,
                    userId,
                    couponId,
                    couponCode,
                    discount,
                    cashback,
                  });
                } else {
                  console.log("No coupon applied to SERVICE order:", orderId);
                }

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
                 * FINAL SERVICE PAYMENT LOG
                 * =================================================
                 */

                console.log("SERVICE PAYMENT PROCESSED SUCCESSFULLY:", {
                  orderId,
                  paymentId: data.paymentId,
                  userId,
                  bookingId: servicePaymentOrder.bookingId,
                  totalAmount,
                  payableAmount,
                  walletAmount,
                  discount,
                  cashbackCredited: cashbackAmount,
                  couponId,
                  couponCode,
                  couponType,
                  previousBalance,
                  balanceAfterDeduction,
                  finalBalance,
                });

                return;
              }
              /**
               * =================================================
               * PUJA PAYMENT
               * =================================================
               */

              if (data.serviceType === "PUJA") {
                console.log("Processing PUJA payment");

                const orderId = data.orderId;

                if (!orderId) {
                  throw new Error("Missing Razorpay order ID");
                }

                // Process only captured payments.
                if (data.status !== "captured") {
                  if (data.status === "failed") {
                    await tx.pujaPaymentOrder.updateMany({
                      where: {
                        razorpayOrderId: orderId,
                        status: {
                          not: "PAID",
                        },
                      },
                      data: {
                        status: "FAILED",
                      },
                    });

                    console.log("PUJA payment failed:", orderId);
                    return;
                  }

                  console.log("Ignoring PUJA payment status:", data.status);
                  return;
                }

                // Fetch payment order and coupon.
                const pujaOrder = await tx.pujaPaymentOrder.findUnique({
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

                // Prevent duplicate processing.
                if (pujaOrder.status === "PAID") {
                  console.log("PUJA payment already processed:", orderId);
                  return;
                }

                // Read stored payment amounts.
                const totalAmount = Number(pujaOrder.totalAmount ?? 0);
                const payableAmount = Number(pujaOrder.payableAmount ?? 0);
                const discount = Number(pujaOrder.discount ?? 0);
                const cashback = Number(pujaOrder.cashback ?? 0);

                const couponId = pujaOrder.couponId ?? null;
                const couponCode = pujaOrder.coupon?.code ?? null;

                const couponType = String(
                  pujaOrder.coupon?.type ?? data.couponType ?? "",
                ).toUpperCase();

                console.log("Processing PUJA payment:", {
                  orderId,
                  bookingId: pujaOrder.bookingId,
                  userId: pujaOrder.userId,
                  totalAmount,
                  payableAmount,
                  discount,
                  cashback,
                  couponId,
                  couponCode,
                  couponType,
                });

                // Validate amounts.
                const amounts = {
                  totalAmount,
                  payableAmount,
                  discount,
                  cashback,
                };

                for (const [key, value] of Object.entries(amounts)) {
                  if (!Number.isFinite(value) || value < 0) {
                    throw new Error(
                      `Invalid PUJA payment amount: ${key}=${value}, orderId=${orderId}`,
                    );
                  }
                }

                // Determine applicable coupon benefits.
                const isCashbackCoupon =
                  Boolean(couponId) &&
                  couponType === "CASHBACK" &&
                  cashback > 0;

                const isDiscountCoupon =
                  Boolean(couponId) &&
                  couponType === "DISCOUNT" &&
                  discount > 0;

                const cashbackAmount = isCashbackCoupon ? cashback : 0;
                const discountAmount = isDiscountCoupon ? discount : 0;

                /*
                 * CLAIM PAYMENT ORDER
                 *
                 * All operations below should execute within the same Prisma transaction.
                 */
                const claimedOrder = await tx.pujaPaymentOrder.updateMany({
                  where: {
                    id: pujaOrder.id,
                    status: {
                      not: "PAID",
                    },
                  },
                  data: {
                    status: "PAID",
                  },
                });

                if (claimedOrder.count === 0) {
                  console.log("PUJA payment already claimed:", orderId);
                  return;
                }

                /*
                 * WALLET TRANSACTIONS
                 *
                 * PUJA payment is not deducted from the wallet.
                 *
                 * CASHBACK:
                 *   - Increase wallet balance.
                 *   - Create CASHBACK transaction.
                 *
                 * DISCOUNT:
                 *   - Create DISCOUNT transaction for history/reporting.
                 *   - Do not change wallet balance.
                 *
                 * If neither benefit applies, do not create a wallet or transaction.
                 */
                if (cashbackAmount > 0 || discountAmount > 0) {
                  const wallet = await tx.userWallet.upsert({
                    where: {
                      userId: pujaOrder.userId,
                    },
                    update: {},
                    create: {
                      userId: pujaOrder.userId,
                      balanceCoins: 0,
                      lockedCoins: 0,
                    },
                  });

                  /*
                   * CREDIT CASHBACK
                   */
                  if (cashbackAmount > 0) {
                    const updatedWallet = await tx.userWallet.update({
                      where: {
                        id: wallet.id,
                      },
                      data: {
                        balanceCoins: {
                          increment: cashbackAmount,
                        },
                      },
                    });

                    await tx.walletTransaction.create({
                      data: {
                        userWalletId: wallet.id,
                        type: "CASHBACK",
                        coins: cashbackAmount,
                        amount: 0,
                        description: `Puja cashback (${couponCode ?? couponId}) - Order ${orderId}`,
                        updatedBalance: updatedWallet.balanceCoins,
                      },
                    });

                    console.log("PUJA CASHBACK CREDITED:", {
                      orderId,
                      userId: pujaOrder.userId,
                      couponCode,
                      cashbackAmount,
                      updatedBalance: updatedWallet.balanceCoins,
                    });
                  }

                  /*
                   * RECORD DISCOUNT
                   *
                   * This is a history record only.
                   * It does not credit or deduct wallet coins.
                   */
                  if (discountAmount > 0) {
                    const discountDescription = `Puja discount (${couponCode ?? couponId}) - Order ${orderId}`;

                    // Prevent duplicate discount entries for this order.
                    const existingDiscountTransaction =
                      await tx.walletTransaction.findFirst({
                        where: {
                          userWalletId: wallet.id,
                          type: "DISCOUNT",
                          description: discountDescription,
                        },
                      });

                    if (!existingDiscountTransaction) {
                      const currentWallet = await tx.userWallet.findUnique({
                        where: {
                          id: wallet.id,
                        },
                      });

                      await tx.walletTransaction.create({
                        data: {
                          userWalletId: wallet.id,
                          type: "DISCOUNT",
                          coins: discountAmount,
                          amount: 0,
                          description: discountDescription,
                          updatedBalance: currentWallet.balanceCoins,
                        },
                      });

                      console.log("PUJA DISCOUNT RECORDED:", {
                        orderId,
                        userId: pujaOrder.userId,
                        couponCode,
                        discountAmount,
                        walletBalance: currentWallet.balanceCoins,
                      });
                    } else {
                      console.log(
                        "PUJA discount transaction already exists:",
                        orderId,
                      );
                    }
                  }
                } else {
                  console.log("No PUJA cashback or discount to record:", {
                    orderId,
                    couponCode,
                    couponType,
                  });
                }

                /*
                 * REDEEM APPLIED PUJA COUPON
                 */
                if (couponId) {
                  console.log("Redeeming PUJA coupon:", {
                    orderId,
                    bookingId: pujaOrder.bookingId,
                    userId: pujaOrder.userId,
                    couponId,
                    couponCode,
                  });

                  await redeemPujaCoupon(tx, data, {
                    ...pujaOrder,
                    couponId,
                  });

                  console.log("PUJA coupon redeemed successfully:", {
                    orderId,
                    couponId,
                    couponCode,
                  });
                } else {
                  console.log("No coupon applied to PUJA order:", orderId);
                }

                /*
                 * UPDATE PUJA BOOKING PAYMENT STATUS
                 */
                await tx.pujaBooking.update({
                  where: {
                    id: pujaOrder.bookingId,
                  },
                  data: {
                    paymentStatus: "PAID",
                    bookingStatus: "CONFIRMED",
                  },
                });

                console.log("PUJA PAYMENT PROCESSED SUCCESSFULLY:", {
                  orderId,
                  bookingId: pujaOrder.bookingId,
                  userId: pujaOrder.userId,
                  totalAmount,
                  payableAmount,
                  discountRecorded: discountAmount,
                  cashbackCredited: cashbackAmount,
                  couponId,
                  couponCode,
                });

                // Prevent this PUJA payment from falling through to recharge logic.
                return;
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
