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
 * GST Configuration
 */
const GST_RATE = Number(process.env.GST_RATE || 18);
const COMPANY_STATE = process.env.COMPANY_STATE || "Delhi";

/**
 * Calculate GST (GST Inclusive)
 */
function calculateGST(totalAmount, customerState) {
  const taxableAmount = Number(
    ((totalAmount * 100) / (100 + GST_RATE)).toFixed(2),
  );

  const totalTax = Number((totalAmount - taxableAmount).toFixed(2));

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
    totalAmount,
  };
}

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
     * Ensure queue exists
     */
    await channel.assertQueue(QUEUE_NAME, {
      durable: true,
    });

    /**
     * Prevent parallel processing
     */
    channel.prefetch(1);

    console.log(`Waiting for messages in queue: ${QUEUE_NAME}`);

    channel.consume(
      QUEUE_NAME,
      async (msg) => {
        if (!msg) return;

        let data;

        /**
         * Parse RabbitMQ message
         */
        try {
          data = JSON.parse(msg.content.toString());
        } catch (err) {
          console.error("Invalid JSON message");

          channel.nack(msg, false, false);
          return;
        }

        console.log("Received message:", data);

        try {
          await prisma.$transaction(
            async (tx) => {
              /**
               * Find payment order
               */

              if (data.serviceType === "SERVICE") {
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

                // Failed payment
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

                // Ignore non-captured payments
                if (data.status !== "captured") {
                  console.log(
                    `Ignoring unsupported service payment status: ${data.status}`,
                  );

                  return;
                }

                // Prevent duplicate processing
                if (servicePaymentOrder.status === "PAID") {
                  console.log(
                    `Service payment already processed: ${data.orderId}`,
                  );

                  return;
                }

                // Mark payment order paid
                await tx.servicePaymentOrder.update({
                  where: {
                    id: servicePaymentOrder.id,
                  },
                  data: {
                    status: "PAID",
                  },
                });

                // Update booking
                await tx.serviceBooking.update({
                  where: {
                    id: servicePaymentOrder.bookingId,
                  },
                  data: {
                    paymentStatus: "SUCCESS",
                    bookingStatus: "ASSIGNED", // or ASSIGNED
                  },
                });

                console.log(
                  `SERVICE PAYMENT SUCCESS booking=${servicePaymentOrder.bookingId}`,
                );

                return;
              }
              const paymentOrder = await tx.paymentOrder.findUnique({
                where: {
                  razorpayOrderId: data.orderId,
                },
              });

              if (!paymentOrder) {
                throw new Error(`Payment order not found: ${data.orderId}`);
              }

              /**
               * HANDLE FAILED PAYMENT
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
               * ONLY HANDLE CAPTURED PAYMENTS
               */
              if (data.status !== "captured") {
                console.log(
                  `Ignoring unsupported payment status: ${data.status}`,
                );

                return;
              }

              /**
               * Prevent duplicate payment processing
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
               * Update payment order status
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
               * Create payment record
               */
              // Calculate GST
              const gst = calculateGST(data.amount, data.state);

              // Generate Invoice Number
              const invoiceNo = `INV-${new Date().getFullYear()}-${Date.now()}`;
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

                  country: data.country,
                  state: data.state,
                  city: data.city,

                  platform: data.platform,
                },
              });

              /**
               * Upsert user wallet
               */
              const totalCoins =
                data.coins +
                (data.couponType === "CASHBACK" ? data.cashback : 0);

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
               * Prevent duplicate wallet transaction
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
               * Create wallet transaction
               */
              await tx.walletTransaction.create({
                data: {
                  userWalletId: wallet.id,
                  rechargePackId: data.rechargePackId,
                  paymentId: payment.id,

                  type: "CREDIT",

                  coins: data.coins,
                  amount: data.amount,

                  description: "Recharge successful",
                },
              });

              // ======================
              // CASHBACK
              // ======================
              if (data.couponType === "CASHBACK" && Number(data.cashback) > 0) {
                const cashbackExists = await tx.walletTransaction.findFirst({
                  where: {
                    paymentId: payment.id,
                    type: "CASHBACK",
                  },
                });

                if (!cashbackExists) {
                  // Credit cashback coins
                  await tx.userWallet.update({
                    where: { id: wallet.id },
                    data: {
                      balanceCoins: {
                        increment: Number(data.cashback),
                      },
                    },
                  });

                  await tx.walletTransaction.create({
                    data: {
                      userWalletId: wallet.id,
                      paymentId: payment.id,
                      rechargePackId: data.rechargePackId,

                      type: "CASHBACK",

                      coins: Number(data.cashback),
                      amount: 0,

                      description: `Cashback (${data.couponCode})`,
                    },
                  });
                }
              }

              // ======================
              // DISCOUNT
              // ======================
              if (data.couponType === "DISCOUNT" && Number(data.discount) > 0) {
                const discountExists = await tx.walletTransaction.findFirst({
                  where: {
                    paymentId: payment.id,
                    type: "DISCOUNT",
                  },
                });

                if (!discountExists) {
                  await tx.walletTransaction.create({
                    data: {
                      userWalletId: wallet.id,
                      paymentId: payment.id,
                      rechargePackId: data.rechargePackId,

                      type: "DISCOUNT",

                      coins: Number(data.discount),
                      amount: 0,

                      description: `Discount Applied (${data.couponCode})`,
                    },
                  });
                }
              }

              console.log(
                `SUCCESS: user=${data.userId}, coins=${data.coins}, payment=${data.paymentId}`,
              );
            },
            {
              timeout: 10000,
            },
          );

          /**
           * ACK message
           */
          channel.ack(msg);
        } catch (err) {
          console.error("Processing failed:", err);

          /**
           * Reject message (no requeue)
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
     * Retry connection after 5 sec
     */
    setTimeout(startConsumer, 5000);
  }
}

/**
 * Graceful shutdown
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

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

/**
 * Start Consumer
 */
startConsumer();
