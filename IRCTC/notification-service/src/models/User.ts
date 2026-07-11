const mongoose = require("mongoose");

const qaHistorySchema = new mongoose.Schema(
  {
    userId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "User",
      required: true,
      index: true,
    },
    question: {
      type: String,
      required: true,
    },
    answer: {
      type: String,
      required: true,
    },
    sources: {
      type: [String],
      default: [],
    },
    confidence: {
      type: String,
      enum: ["high", "medium", "low"],
      required: true,
    },
    latencyMs: {
      type: Number,
    },
    createdAt: {
      type: Date,
      default: Date.now,
    },
  },
  { timestamps: false },
);

export const QAHistory = mongoose.model("QAHistory", qaHistorySchema);
