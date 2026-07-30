-- CreateEnum
CREATE TYPE "SeatStatus" AS ENUM ('AVAILABLE', 'LOCKED', 'BOOKED', 'CANCELLED');

-- CreateTable
CREATE TABLE "schedule_inventories" (
    "id" TEXT NOT NULL,
    "scheduleId" TEXT NOT NULL,
    "trainId" TEXT NOT NULL,
    "trainNumber" TEXT NOT NULL,
    "trainName" TEXT NOT NULL,
    "departureDate" DATE NOT NULL,
    "totalSeats" INTEGER NOT NULL,
    "available" INTEGER NOT NULL,
    "locked" INTEGER NOT NULL DEFAULT 0,
    "booked" INTEGER NOT NULL DEFAULT 0,
    "status" TEXT NOT NULL DEFAULT 'ACTIVE',
    "version" INTEGER NOT NULL DEFAULT 0,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "schedule_inventories_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "seat_inventories" (
    "id" TEXT NOT NULL,
    "scheduleInventoryId" TEXT NOT NULL,
    "scheduleId" TEXT NOT NULL,
    "seatId" TEXT NOT NULL,
    "seatNumber" INTEGER NOT NULL,
    "seatType" TEXT NOT NULL,
    "price" DOUBLE PRECISION NOT NULL,
    "status" "SeatStatus" NOT NULL DEFAULT 'AVAILABLE',
    "lockedBy" TEXT,
    "lockedAt" TIMESTAMP(3),
    "lockExpiresAt" TIMESTAMP(3),
    "bookingId" TEXT,
    "version" INTEGER NOT NULL DEFAULT 0,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "seat_inventories_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "route_stops" (
    "id" TEXT NOT NULL,
    "scheduleId" TEXT NOT NULL,
    "stationId" TEXT NOT NULL,
    "stationName" TEXT NOT NULL,
    "stationCode" TEXT NOT NULL,
    "sequenceNumber" INTEGER NOT NULL,

    CONSTRAINT "route_stops_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "seat_segment_locks" (
    "id" TEXT NOT NULL,
    "scheduleId" TEXT NOT NULL,
    "seatId" TEXT NOT NULL,
    "fromSeq" INTEGER NOT NULL,
    "toSeq" INTEGER NOT NULL,
    "status" "SeatStatus" NOT NULL DEFAULT 'LOCKED',
    "lockedBy" TEXT,
    "lockedAt" TIMESTAMP(3),
    "lockExpiresAt" TIMESTAMP(3),
    "bookingId" TEXT,
    "version" INTEGER NOT NULL DEFAULT 0,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "seat_segment_locks_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "idempotency_records" (
    "id" TEXT NOT NULL,
    "eventKey" TEXT NOT NULL,
    "processedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "idempotency_records_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "schedule_inventories_scheduleId_key" ON "schedule_inventories"("scheduleId");

-- CreateIndex
CREATE INDEX "schedule_inventories_trainId_idx" ON "schedule_inventories"("trainId");

-- CreateIndex
CREATE INDEX "schedule_inventories_scheduleId_status_idx" ON "schedule_inventories"("scheduleId", "status");

-- CreateIndex
CREATE INDEX "schedule_inventories_departureDate_idx" ON "schedule_inventories"("departureDate");

-- CreateIndex
CREATE INDEX "seat_inventories_scheduleId_status_idx" ON "seat_inventories"("scheduleId", "status");

-- CreateIndex
CREATE INDEX "seat_inventories_lockExpiresAt_status_idx" ON "seat_inventories"("lockExpiresAt", "status");

-- CreateIndex
CREATE INDEX "seat_inventories_bookingId_idx" ON "seat_inventories"("bookingId");

-- CreateIndex
CREATE UNIQUE INDEX "seat_inventories_scheduleId_seatId_key" ON "seat_inventories"("scheduleId", "seatId");

-- CreateIndex
CREATE UNIQUE INDEX "seat_inventories_scheduleId_seatNumber_key" ON "seat_inventories"("scheduleId", "seatNumber");

-- CreateIndex
CREATE INDEX "route_stops_scheduleId_idx" ON "route_stops"("scheduleId");

-- CreateIndex
CREATE UNIQUE INDEX "route_stops_scheduleId_stationId_key" ON "route_stops"("scheduleId", "stationId");

-- CreateIndex
CREATE UNIQUE INDEX "route_stops_scheduleId_sequenceNumber_key" ON "route_stops"("scheduleId", "sequenceNumber");

-- CreateIndex
CREATE INDEX "seat_segment_locks_scheduleId_seatId_idx" ON "seat_segment_locks"("scheduleId", "seatId");

-- CreateIndex
CREATE INDEX "seat_segment_locks_scheduleId_seatId_status_idx" ON "seat_segment_locks"("scheduleId", "seatId", "status");

-- CreateIndex
CREATE INDEX "seat_segment_locks_lockExpiresAt_status_idx" ON "seat_segment_locks"("lockExpiresAt", "status");

-- CreateIndex
CREATE INDEX "seat_segment_locks_bookingId_idx" ON "seat_segment_locks"("bookingId");

-- CreateIndex
CREATE UNIQUE INDEX "idempotency_records_eventKey_key" ON "idempotency_records"("eventKey");

-- CreateIndex
CREATE INDEX "idempotency_records_eventKey_idx" ON "idempotency_records"("eventKey");

-- AddForeignKey
ALTER TABLE "seat_inventories" ADD CONSTRAINT "seat_inventories_scheduleInventoryId_fkey" FOREIGN KEY ("scheduleInventoryId") REFERENCES "schedule_inventories"("id") ON DELETE CASCADE ON UPDATE CASCADE;
