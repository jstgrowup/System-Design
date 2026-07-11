import logger from "../config/logger";

import {
  getOtpTemplate,
  getWelcomeTemplate,
  getTicketConfirmationTemplate,
  getBookingConfirmedTemplate,
  getBookingFailedTemplate,
  getBookingCancelledTemplate,
  BookingConfirmedData,
  BookingFailedData,
  BookingCancelledData,
} from "../templates";
import { config } from "../config/config";
import { Resend } from "resend";

// 2.11
const resend = new Resend(config.RESEND_API_KEY ?? "");

interface SendResult {
  success: boolean;
}

interface EmailMessage {
  to: string;
  from: string;
  subject: string;
  html: string;
}

class EmailService {
  private from: string;
  private maxRetries: number;

  constructor() {
    this.from = config.MAIL_SEND ?? "";
    this.maxRetries = 3;
  }

  private async sendWithRetry(
    msg: EmailMessage,
    retries = 0,
  ): Promise<SendResult> {
    try {
      const { data, error } = await resend.emails.send({
        from: msg.from,
        to: msg.to,
        subject: msg.subject,
        html: msg.html,
      });

      if (error) {
        throw new Error(error.message);
      }

      logger.info(`Email sent successfully to ${msg.to}`, {
        subject: msg.subject,
        attempt: retries + 1,
        id: data?.id,
      });
      return { success: true };
    } catch (error: any) {
      logger.error(
        `Email sending failed (attempt ${retries + 1}/${this.maxRetries})`,
        {
          to: msg.to,
          error: error.message,
        },
      );

      if (retries < this.maxRetries - 1) {
        const delay = Math.pow(2, retries) * 1000;
        await new Promise((resolve) => setTimeout(resolve, delay));
        return this.sendWithRetry(msg, retries + 1);
      }

      throw error;
    }
  }

  async sendOtpEmail(
    email: string,
    otp: string,
    ttlMinutes: number,
  ): Promise<SendResult> {
    const msg: EmailMessage = {
      to: email,
      from: this.from,
      subject: "Your DesignKarle verification code",
      html: getOtpTemplate(otp, ttlMinutes),
    };
    return this.sendWithRetry(msg);
  }

  async sendWelcomeEmail(
    email: string,
    firstName: string,
  ): Promise<SendResult> {
    const msg: EmailMessage = {
      to: email,
      from: this.from,
      subject: "Welcome to DesignKarle - Email Verified",
      html: getWelcomeTemplate(firstName),
    };
    return this.sendWithRetry(msg);
  }

  async sendBookingConfirmedEmail(
    email: string,
    bookingData: BookingConfirmedData,
  ): Promise<SendResult> {
    const msg: EmailMessage = {
      to: email,
      from: this.from,
      subject: `Booking Confirmed - ${bookingData.trainName || "Your Train Ticket"}`,
      html: getBookingConfirmedTemplate(bookingData),
    };
    return this.sendWithRetry(msg);
  }

  async sendBookingFailedEmail(
    email: string,
    bookingData: BookingFailedData,
  ): Promise<SendResult> {
    const msg: EmailMessage = {
      to: email,
      from: this.from,
      subject: "Booking Unsuccessful - Please Try Again",
      html: getBookingFailedTemplate(bookingData),
    };
    return this.sendWithRetry(msg);
  }

  async sendBookingCancelledEmail(
    email: string,
    bookingData: BookingCancelledData,
  ): Promise<SendResult> {
    const msg: EmailMessage = {
      to: email,
      from: this.from,
      subject: "Booking Cancelled - Refund Update",
      html: getBookingCancelledTemplate(bookingData),
    };
    return this.sendWithRetry(msg);
  }
}

export default new EmailService();
