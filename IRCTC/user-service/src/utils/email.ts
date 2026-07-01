import sgMail, { MailDataRequired } from "@sendgrid/mail";
import logger from "../config/logger";
import { config } from "../config";
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
} from "./templates";

sgMail.setApiKey(config.SENDGRID_API_KEY ?? "");

interface SendResult {
  success: boolean;
}

class EmailService {
  private from: string;
  private maxRetries: number;

  constructor() {
    this.from = config.MAIL_SEND ?? "";
    this.maxRetries = 3;
  }

  private async sendWithRetry(
    msg: MailDataRequired,
    retries = 0,
  ): Promise<SendResult> {
    try {
      await sgMail.send(msg);
      logger.info(`Email sent successfully to ${msg.to}`, {
        subject: msg.subject,
        attempt: retries + 1,
      });
      return { success: true };
    } catch (error: any) {
      logger.error(
        `Email sending failed (attempt ${retries + 1}/${this.maxRetries})`,
        {
          to: msg.to,
          error: error.message,
          code: error.code,
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
    const msg: MailDataRequired = {
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
    const msg: MailDataRequired = {
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
    const msg: MailDataRequired = {
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
    const msg: MailDataRequired = {
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
    const msg: MailDataRequired = {
      to: email,
      from: this.from,
      subject: "Booking Cancelled - Refund Update",
      html: getBookingCancelledTemplate(bookingData),
    };
    return this.sendWithRetry(msg);
  }
}

export default new EmailService();
