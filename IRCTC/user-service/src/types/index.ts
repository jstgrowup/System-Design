import { Types } from "mongoose";

export interface KnowledgeDoc {
  _id: Types.ObjectId | string;
  title: string;
  content: string;
  tags: string[];
  createdAt: Date;
  embedding: number[];
  sim?: number;
}

export interface RAGResponse {
  answer: string;
  sources: { id: string; title: string }[];
  confidence: "high" | "medium" | "low";
}
