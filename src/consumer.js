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

  const totalTax = Number(
    (numericTotalAmount - taxableAmount).toFixed(2),
  );

  console.log("GST taxableAmount:", taxableAmount);
  console.log("GST totalTax:", totalTax);

  let cgst = 0;
  let sgst = 0;
  let igst = 0;

  if (
    customerState &&
    customerState.trim().toLowerCase() ===
      COMPANY_STATE.trim().toLowerCase()
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

  const pgCharge = Number(
    (numericTotalAmount * (PG_RATE / 100)).toFixed(2),
  );

  const pgIgst = Number(
    (pgCharge * (PG_GST_RATE / 100)).toFixed(2),
  );

  const pgTotal = Number(
    (pgCharge + pgIgst).toFixed(2),
  );

  const receivableAmount = Number(
    (numericTotalAmount - pgTotal).toFixed(2),
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
 * ============================
 * START CONSUMER
 * ============================
 */
async function startConsumer() {
  try {
    console.log("Connecting to RabbitMQ...");

    connection = await amqp.connect(process.env.RABBITMQ_URL);

    connection.on("error", (err) => {
      console.error(
        "RabbitMQ connection error:",
        err.message,
      );
    });

    connection.on("close", () => {
      console.warn(
        "RabbitMQ connection closed. Reconnecting...",
      );

      setTimeout(startConsumer, 5000);
    });

    channel = await connection.createChannel();

    /**
     * Ensure queue exists
     */
    await channel.assertQueue(QUEUE_NAME, {
      durable: true,
    });

    /**
     * Prevent parallel processing
     */
    channel.prefetch(1);

    console.log(
      `Waiting for messages in queue: ${QUEUE_NAME}`,
    );

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
              if (data.serviceType === "SERVICE") {
                const servicePaymentOrder =
                  await tx.servicePaymentOrder.findUnique({
                    where: {
                      razorpayOrderId:
                        data.orderId,
                    },
                  });

                if (!servicePaymentOrder) {
                  throw new Error(
                    `Service payment order not found: ${data.orderId}`,
                  );
                }

                /**
                 * FAILED PAYMENT
                 */
                if (
                  data.status === "failed"
                ) {
                  await tx.servicePaymentOrder.update({
                    where: {
                      id: servicePaymentOrder.id,
                    },
                    data: {
                      status: "FAILED",
                    },
                  });

                  console.log(
                    `SERVICE PAYMENT FAILED: order=${data.orderId}`,
                  );

                  return;
                }

                /**
                 * IGNORE NON-CAPTURED PAYMENT
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
                 * PREVENT DUPLICATE PROCESSING
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
                 * MARK PAYMENT ORDER PAID
                 */
                await tx.servicePaymentOrder.update({
                  where: {
                    id: servicePaymentOrder.id,
                  },
                  data: {
                    status: "PAID",
                  },
                });

                /**
                 * UPDATE BOOKING
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

                console.log(
                  `SERVICE PAYMENT SUCCESS booking=${servicePaymentOrder.bookingId}`,
                );

                return;
              }

              /**
               * =================================================
               * NORMAL WALLET / RECHARGE PAYMENT
               * =================================================
               */

              const paymentOrder =
                await tx.paymentOrder.findUnique({
                  where: {
                    razorpayOrderId:
                      data.orderId,
                  },
                });

              if (!paymentOrder) {
                throw new Error(
                  `Payment order not found: ${data.orderId}`,
                );
              }

              /**
               * ============================
               * FAILED PAYMENT
               * ============================
               */
              if (
                data.status === "failed"
              ) {
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
               * ============================
               * ONLY HANDLE CAPTURED
               * ============================
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
               * ============================
               * PREVENT DUPLICATE PAYMENT
               * ============================
               */
              const existingPayment =
                await tx.payment.findUnique({
                  where: {
                    razorpayPaymentId:
                      data.paymentId,
                  },
                });

              if (existingPayment) {
                console.log(
                  `Payment already processed: ${data.paymentId}`,
                );

                return;
              }

              /**
               * ============================
               * UPDATE PAYMENT ORDER
               * ============================
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
               * ============================
               * CALCULATE GST
               * ============================
               */
              const gst =
                calculateGST(
                  data.amount,
                  data.state,
                );

              /**
               * ============================
               * CALCULATE PG CHARGES
               * ============================
               */
              const pg =
                calculatePGCharges(
                  data.amount,
                );

              /**
               * ============================
               * GENERATE INVOICE NUMBER
               * ============================
               */
              const invoiceNo =
                `INV-${new Date().getFullYear()}-${Date.now()}`;

              console.log(
                "taxableAmount:",
                gst.taxableAmount,
              );

              /**
               * ============================
               * CREATE PAYMENT RECORD
               * ============================
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
               * =================================================
               * COUPON REDEMPTION
               * =================================================
               *
               * IMPORTANT:
               *
               * Coupon is redeemed ONLY after payment
               * has successfully been captured.
               *
               * We do NOT create CouponRedemption when
               * user only applies the coupon.
               */
              if (data.couponId) {
                console.log(
                  "Processing coupon redemption:",
                  {
                    couponId:
                      data.couponId,

                    couponCode:
                      data.couponCode,

                    couponType:
                      data.couponType,

                    userId:
                      data.userId,

                    discount:
                      data.discount,
                  },
                );

                /**
                 * Find existing redemption
                 */
                const existingRedemption =
                  await tx.couponRedemption.findFirst({
                    where: {
                      couponId:
                        data.couponId,

                      userId:
                        data.userId,
                    },
                  });

                /**
                 * Create redemption only
                 * if user has not used coupon.
                 */
                if (
                  !existingRedemption
                ) {
                  await tx.couponRedemption.create({
                    data: {
                      couponId:
                        data.couponId,

                      userId:
                        data.userId,

                      paymentOrderId:
                        paymentOrder.id,

                      discount:
                        data.couponType ===
                        "DISCOUNT"
                          ? Number(
                              data.discount ||
                                0,
                            )
                          : 0,
                    },
                  });

                  /**
                   * Increment coupon usedCount
                   */
                  await tx.coupon.update({
                    where: {
                      id: data.couponId,
                    },

                    data: {
                      usedCount: {
                        increment: 1,
                      },
                    },
                  });

                  console.log(
                    `COUPON REDEEMED SUCCESSFULLY: coupon=${data.couponCode}, couponId=${data.couponId}, user=${data.userId}`,
                  );
                } else {
                  console.log(
                    `COUPON ALREADY REDEEMED: coupon=${data.couponCode}, couponId=${data.couponId}, user=${data.userId}`,
                  );
                }
              }

              /**
               * =================================================
               * WALLET CREDIT
               * =================================================
               *
               * IMPORTANT:
               *
               * Only recharge coins are credited here.
               *
               * Cashback is credited separately below.
               *
               * This prevents cashback from being credited twice.
               */
              const totalCoins =
                Number(
                  data.coins || 0,
                );

              const wallet =
                await tx.userWallet.upsert({
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

                    lockedCoins:
                      0,
                  },
                });

              /**
               * ============================
               * PREVENT DUPLICATE WALLET TX
               * ============================
               */
              const existingWalletTx =
                await tx.walletTransaction.findFirst({
                  where: {
                    paymentId:
                      payment.id,

                    type:
                      "CREDIT",
                  },
                });

              if (
                existingWalletTx
              ) {
                console.log(
                  `Wallet transaction already exists for payment=${payment.id}`,
                );

                return;
              }

              /**
               * ============================
               * CREATE CREDIT TRANSACTION
               * ============================
               */
              await tx.walletTransaction.create({
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
              });

              /**
               * =================================================
               * CASHBACK
               * =================================================
               */
              if (
                data.couponType ===
                  "CASHBACK" &&
                Number(
                  data.cashback || 0,
                ) > 0
              ) {
                const cashbackExists =
                  await tx.walletTransaction.findFirst({
                    where: {
                      paymentId:
                        payment.id,

                      type:
                        "CASHBACK",
                    },
                  });

                if (
                  !cashbackExists
                ) {
                  const cashbackAmount =
                    Number(
                      data.cashback ||
                        0,
                    );

                  /**
                   * Credit cashback
                   */
                  await tx.userWallet.update({
                    where: {
                      id: wallet.id,
                    },

                    data: {
                      balanceCoins: {
                        increment:
                          cashbackAmount,
                      },
                    },
                  });

                  /**
                   * Cashback transaction
                   */
                  await tx.walletTransaction.create({
                    data: {
                      userWalletId:
                        wallet.id,

                      paymentId:
                        payment.id,

                      rechargePackId:
                        data.rechargePackId,

                      type:
                        "CASHBACK",

                      coins:
                        cashbackAmount,

                      amount:
                        0,

                      description:
                        `Cashback (${data.couponCode || "Coupon"})`,
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
                data.couponType ===
                  "DISCOUNT" &&
                Number(
                  data.discount || 0,
                ) > 0
              ) {
                const discountExists =
                  await tx.walletTransaction.findFirst({
                    where: {
                      paymentId:
                        payment.id,

                      type:
                        "DISCOUNT",
                    },
                  });

                if (
                  !discountExists
                ) {
                  const discountAmount =
                    Number(
                      data.discount ||
                        0,
                    );

                  await tx.walletTransaction.create({
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

                      amount:
                        0,

                      description:
                        `Discount Applied (${data.couponCode || "Coupon"})`,
                    },
                  });

                  console.log(
                    `DISCOUNT APPLIED: amount=${discountAmount}, coupon=${data.couponCode}`,
                  );
                }
              }

              /**
               * ============================
               * SUCCESS LOG
               * ============================
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
          console.error(
            "Processing failed:",
            err,
          );

          /**
           * ============================
           * REJECT MESSAGE
           * ============================
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
     * Retry connection after 5 seconds
     */
    setTimeout(
      startConsumer,
      5000,
    );
  }
}

/**
 * ============================
 * GRACEFUL SHUTDOWN
 * ============================
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
 * ============================
 * PROCESS SIGNALS
 * ============================
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
 * ============================
 * START CONSUMER
 * ============================
 */
startConsumer();