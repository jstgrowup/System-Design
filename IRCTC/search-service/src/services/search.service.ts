import { esClient, TRAIN_INDEX, STATION_INDEX } from "../config/elasticsearch";
import logger from "../config/logger";
// `import type` only — these are erased at compile time, so importing them
// doesn't create a runtime circular dependency with kafka/search.service.ts
// (which imports this module's default export by value).
import type {
  StationCreatedEvent,
  RouteCreatedEvent,
  ScheduleCreatedEvent,
  ScheduleCancelledEvent,
  SeatAvailabilityUpdatedEvent,
} from "../kafka/search.service";

/** Narrows a caught `unknown` down to a loggable string without an `as Error` assertion. */
const errorMessage = (err: unknown): string =>
  err instanceof Error ? err.message : String(err);

// ── Elasticsearch document shapes (what we actually read/write) ──

/** Per-seatType counts stored on a train document, plus a running `total`. */
interface SeatSummary {
  total: number;
  LOWER: number;
  MIDDLE: number;
  UPPER: number;
  SIDE_LOWER: number;
  SIDE_UPPER: number;
  // Indexable by seatType (a plain string on the incoming event) while still
  // requiring the named counters above — lets indexTrainRoute increment
  // seatSummary[s.seatType] without a cast.
  [seatType: string]: number;
}

/** One entry in a train document's `schedules` array — a single departureDate run. */
interface ScheduleSummary {
  scheduleId: string;
  departureDate: string;
  status: string;
  available: number;
  locked: number;
  booked: number;
}

/** One stop in a train document's nested `route` array. */
interface RouteStop {
  stationId: string;
  stationName: string;
  stationCode: string;
  sequenceNumber: number;
  arrivalTime: string | null;
  departureTime: string | null;
  distanceFromOrigin: number;
}

/** The full Elasticsearch document stored in the `trains` index. */
interface TrainDocument {
  trainId: string;
  trainNumber: string;
  trainName: string;
  route: RouteStop[];
  schedules: ScheduleSummary[];
  seatSummary: SeatSummary;
}

/** The full Elasticsearch document stored in the `stations` index. */
interface StationDocument {
  stationId: string;
  // Optional: indexStation's own write doesn't currently set this field
  // (only the per-station reindex inside indexTrainRoute does), so a
  // station created but never attached to a route may have no name here.
  name?: string;
  code: string;
  city: string;
  suggest?: { input: string[]; weight: number };
}

// ── Minimal shapes for the parts of Elasticsearch's response we actually
// read. The client's own response generics are deep; for what this file
// needs, asserting the wire response into these narrow local shapes at the
// boundary is clearer than fighting the client's generics, and is exactly
// the kind of external-boundary `as` the type-safety rules carve out. ──

/** A single Elasticsearch hit — `_source` is the original indexed document. */
interface EsHit<TSource> {
  _source?: TSource;
}

/** Response shape for a plain `esClient.search()` call with no inner_hits/suggest. */
interface EsSimpleSearchResult<TSource> {
  hits: { hits: EsHit<TSource>[] };
}

/** The `inner_hits` block attached to a hit when a nested query requests one. */
interface EsInnerHits<TSource> {
  hits: { hits: EsHit<TSource>[] };
}

/**
 * A train-index hit from searchTrains's nested query — carries the matched
 * train document plus the specific `from`/`to` route stops that satisfied
 * the query, via named inner_hits ("from_station"/"to_station").
 */
interface TrainSearchHit extends EsHit<TrainDocument> {
  inner_hits?: {
    from_station?: EsInnerHits<RouteStop>;
    to_station?: EsInnerHits<RouteStop>;
  };
}

/** Response shape for searchTrains's nested from/to query. */
interface TrainSearchResult {
  hits: { hits: TrainSearchHit[] };
}

/** Response shape for a completion-suggester (`suggest`) query. */
interface EsSuggestResult<TSource> {
  suggest?: {
    station_suggest?: { options: EsHit<TSource>[] }[];
  };
}

// ═══════════════════════════════════════════════════
//  INDEX OPERATIONS (called by Kafka consumer)
// ═══════════════════════════════════════════════════

/**
 * When admin creates a station, index it for autocomplete.
 * Event shape: { eventType, data: { id, name, code, city, state }, timestamp }
 */
const indexStation = async (event: StationCreatedEvent): Promise<void> => {
  const station = event.data;
  if (!station) return;

  try {
    await esClient.index({
      index: STATION_INDEX,
      id: station.id,
      document: {
        stationId: station.id,
        code: station.code,
        city: station.city,
        suggest: {
          input: [station.name, station.code, station.city].filter(Boolean),
          weight: 10,
        },
      },
      refresh: true,
    });
    logger.info(`Indexed station ${station.name} (${station.code})`);
  } catch (err) {
    logger.error(`Failed to index station: ${errorMessage(err)}`);
  }
};

/**
 * When admin creates a route, we get enriched payload with train+seats+routeStations.
 */
const indexTrainRoute = async (
  routeEvent: RouteCreatedEvent,
): Promise<void> => {
  const { train, routeStations } = routeEvent;
  if (!train || !routeStations) return;

  const seatSummary: SeatSummary = {
    total: 0,
    LOWER: 0,
    MIDDLE: 0,
    UPPER: 0,
    SIDE_LOWER: 0,
    SIDE_UPPER: 0,
  };
  (train.seats || []).forEach((s) => {
    seatSummary.total++;
    if (seatSummary[s.seatType] !== undefined) seatSummary[s.seatType]++;
  });

  const doc: TrainDocument = {
    trainId: train.id,
    trainNumber: train.trainNumber,
    trainName: train.trainName,
    route: routeStations.map((rs) => ({
      stationId: rs.station.id,
      stationName: rs.station.name,
      stationCode: rs.station.code,
      sequenceNumber: rs.sequenceNumber,
      arrivalTime: rs.arrivalTime,
      departureTime: rs.departureTime,
      distanceFromOrigin: rs.distanceFromOrigin,
    })),
    schedules: [],
    seatSummary,
  };

  await esClient.index({
    index: TRAIN_INDEX,
    id: train.id,
    document: doc,
    refresh: true,
  });

  // Also index/update stations for autocomplete
  for (const rs of routeStations) {
    await esClient.index({
      index: STATION_INDEX,
      id: rs.station.id,
      document: {
        stationId: rs.station.id,
        name: rs.station.name,
        code: rs.station.code,
        city: rs.station.city,
        suggest: {
          input: [rs.station.name, rs.station.code, rs.station.city].filter(
            Boolean,
          ),
          weight: 10,
        },
      },
      refresh: true,
    });
  }

  logger.info(
    `Indexed train ${train.trainNumber} with ${routeStations.length} stations`,
  );
};

/**
 * When admin creates a schedule, add it to the train's schedules array.
 */
const indexSchedule = async (
  scheduleEvent: ScheduleCreatedEvent,
): Promise<void> => {
  const { scheduleId, trainId, departureDate, status, seats } = scheduleEvent;

  const totalSeats = seats ? seats.length : 0;

  try {
    await esClient.update({
      index: TRAIN_INDEX,
      id: trainId,
      script: {
        source: `
            if (ctx._source.schedules == null) { ctx._source.schedules = []; }
            // Remove existing schedule with same id (idempotent)
            ctx._source.schedules.removeIf(s -> s.scheduleId == params.scheduleId);
            ctx._source.schedules.add(params.newSchedule);
          `,
        params: {
          scheduleId,
          newSchedule: {
            scheduleId,
            departureDate,
            status,
            available: totalSeats,
            locked: 0,
            booked: 0,
          },
        },
      },
      refresh: true,
    });
    logger.info(`Indexed schedule ${scheduleId} for train ${trainId}`);
  } catch (err) {
    logger.warn(
      `Could not index schedule for train ${trainId}: ${errorMessage(err)}`,
    );
  }
};

/**
 * When admin cancels a schedule, update its status in ES.
 * Event shape: { eventType, data: { id, trainId, status: 'CANCELLED', ... }, timestamp }
 */
const cancelSchedule = async (event: ScheduleCancelledEvent): Promise<void> => {
  const schedule = event.data;
  if (!schedule) return;

  try {
    await esClient.update({
      index: TRAIN_INDEX,
      id: schedule.trainId,
      script: {
        source: `
            if (ctx._source.schedules != null) {
              for (def s : ctx._source.schedules) {
                if (s.scheduleId == params.scheduleId) {
                  s.status = 'CANCELLED';
                }
              }
            }
          `,
        params: { scheduleId: schedule.id },
      },
      refresh: true,
    });
    logger.info(
      `Cancelled schedule ${schedule.id} for train ${schedule.trainId}`,
    );
  } catch (err) {
    logger.warn(`Could not cancel schedule: ${errorMessage(err)}`);
  }
};

/**
 * When inventory changes (seat booked/released), update availability counts.
 */
const updateSeatAvailability = async (
  event: SeatAvailabilityUpdatedEvent,
): Promise<void> => {
  const { scheduleId, trainId, available, locked, booked } = event;

  try {
    await esClient.update({
      index: TRAIN_INDEX,
      id: trainId,
      script: {
        source: `
            if (ctx._source.schedules != null) {
              for (def s : ctx._source.schedules) {
                if (s.scheduleId == params.scheduleId) {
                  s.available = params.available;
                  s.locked    = params.locked;
                  s.booked    = params.booked;
                }
              }
            }
          `,
        params: {
          scheduleId,
          available: available || 0,
          locked: locked || 0,
          booked: booked || 0,
        },
      },
      refresh: true,
    });
    logger.info(`Updated availability for schedule ${scheduleId}`);
  } catch (err) {
    logger.warn(`Could not update availability: ${errorMessage(err)}`);
  }
};

// ═══════════════════════════════════════════════════
//  SEARCH OPERATIONS (called by API)
// ═══════════════════════════════════════════════════

/** One train in searchTrains's results, with the specific from/to stop it matched on. */
interface SearchTrainMatch {
  trainId: string;
  trainNumber: string;
  trainName: string;
  from: {
    name?: string;
    code: string;
    departure: string | null;
    stationId: string;
    sequenceNumber: number;
  };
  to: {
    name?: string;
    code: string;
    arrival: string | null;
    stationId: string;
    sequenceNumber: number;
  };
  seatSummary: SeatSummary;
  schedule: ScheduleSummary | null;
}

/** searchTrains's return value: either "no such station" or a real result set. */
type SearchTrainsResult =
  | { trains: []; message: string }
  | {
      from: { resolved: string; code: string };
      to: { resolved: string; code: string };
      date: string;
      count: number;
      trains: SearchTrainMatch[];
    };

/**
 * Search trains running between two stations on a given date.
 * Supports fuzzy matching on station names.
 */
const searchTrains = async ({
  from,
  to,
  date,
}: {
  from: string;
  to: string;
  date?: string;
}): Promise<SearchTrainsResult> => {
  const fromStation = await resolveStation(from);
  const toStation = await resolveStation(to);

  if (!fromStation)
    return { trains: [], message: `Station "${from}" not found` };
  if (!toStation) return { trains: [], message: `Station "${to}" not found` };

  const query = {
    bool: {
      must: [
        {
          nested: {
            path: "route",
            query: { term: { "route.stationId": fromStation.stationId } },
            inner_hits: { name: "from_station" },
          },
        },
        {
          nested: {
            path: "route",
            query: { term: { "route.stationId": toStation.stationId } },
            inner_hits: { name: "to_station" },
          },
        },
      ],
    },
  };

  const result = (await esClient.search({
    index: TRAIN_INDEX,
    query,
    size: 50,
  })) as unknown as TrainSearchResult;

  const normalize = (d: string) => new Date(d).toISOString().slice(0, 10);

  const trains = result.hits.hits
    .map((hit): SearchTrainMatch | null => {
      const src = hit._source;
      const fromHit = hit.inner_hits?.from_station?.hits.hits[0]?._source;
      const toHit = hit.inner_hits?.to_station?.hits.hits[0]?._source;

      if (
        !src ||
        !fromHit ||
        !toHit ||
        fromHit.sequenceNumber >= toHit.sequenceNumber
      ) {
        return null;
      }

      let scheduleInfo: ScheduleSummary | null = null;
      if (date && src.schedules && src.schedules.length > 0) {
        scheduleInfo =
          src.schedules.find(
            (s) => s.status === "ACTIVE" && normalize(s.departureDate) === date,
          ) ?? null;
      }

      return {
        trainId: src.trainId,
        trainNumber: src.trainNumber,
        trainName: src.trainName,
        // --- SEGMENT BOOKING: Added stationId and sequenceNumber to from/to for segment-aware booking ---
        from: {
          name: fromHit.stationName,
          code: fromHit.stationCode,
          departure: fromHit.departureTime,
          stationId: fromHit.stationId,
          sequenceNumber: fromHit.sequenceNumber,
        },
        to: {
          name: toHit.stationName,
          code: toHit.stationCode,
          arrival: toHit.arrivalTime,
          stationId: toHit.stationId,
          sequenceNumber: toHit.sequenceNumber,
        },
        seatSummary: src.seatSummary,
        schedule: scheduleInfo,
      };
    })
    .filter((t): t is SearchTrainMatch => t !== null);

  return {
    from: {
      resolved: fromStation.name ?? fromStation.code,
      code: fromStation.code,
    },
    to: { resolved: toStation.name ?? toStation.code, code: toStation.code },
    date: date || "any",
    count: trains.length,
    trains,
  };
};

/**
 * Fuzzy-resolve a station name/code to its ID.
 * Three strategies: exact code → completion suggester → fuzzy match
 */
const resolveStation = async (
  input: string,
): Promise<StationDocument | null> => {
  // 1. Try exact code match
  const exactResult = (await esClient.search({
    index: STATION_INDEX,
    query: { term: { code: input.toUpperCase() } },
    size: 1,
  })) as unknown as EsSimpleSearchResult<StationDocument>;
  if (exactResult.hits.hits.length > 0) {
    return exactResult.hits.hits[0]._source ?? null;
  }

  // 2. Try completion suggester (handles typos like "dehli" → "Delhi")
  try {
    const suggestResult = (await esClient.search({
      index: STATION_INDEX,
      suggest: {
        station_suggest: {
          prefix: input,
          completion: {
            field: "suggest",
            fuzzy: { fuzziness: "AUTO" },
            size: 1,
          },
        },
      },
    })) as unknown as EsSuggestResult<StationDocument>;
    const options = suggestResult.suggest?.station_suggest?.[0]?.options ?? [];
    if (options.length > 0) return options[0]._source ?? null;
  } catch (err) {
    logger.warn(`Suggest fallback failed: ${errorMessage(err)}`);
  }

  // 3. Fuzzy match on name
  const fuzzyResult = (await esClient.search({
    index: STATION_INDEX,
    query: {
      multi_match: {
        query: input,
        fields: ["name", "city"],
        fuzziness: "AUTO",
        prefix_length: 1,
      },
    },
    size: 1,
  })) as unknown as EsSimpleSearchResult<StationDocument>;

  return fuzzyResult.hits.hits.length > 0
    ? (fuzzyResult.hits.hits[0]._source ?? null)
    : null;
};

/**
 * Autocomplete station names as user types.
 */
const autocompleteStation = async (
  prefix: string,
): Promise<{ name?: string; code: string; stationId: string }[]> => {
  const result = (await esClient.search({
    index: STATION_INDEX,
    suggest: {
      station_suggest: {
        prefix,
        completion: {
          field: "suggest",
          fuzzy: { fuzziness: "AUTO" },
          size: 10,
        },
      },
    },
  })) as unknown as EsSuggestResult<StationDocument>;

  const options = result.suggest?.station_suggest?.[0]?.options ?? [];
  return options
    .map((o) => o._source)
    .filter((s): s is StationDocument => s !== undefined)
    .map((s) => ({ name: s.name, code: s.code, stationId: s.stationId }));
};

/**
 * Debug: get all indexed stations
 */
const getAllStations = async (): Promise<StationDocument[]> => {
  const result = (await esClient.search({
    index: STATION_INDEX,
    query: { match_all: {} },
    size: 100,
  })) as unknown as EsSimpleSearchResult<StationDocument>;
  return result.hits.hits
    .map((h) => h._source)
    .filter((s): s is StationDocument => s !== undefined);
};

/**
 * Debug: get all indexed trains
 */
const getAllTrains = async (): Promise<TrainDocument[]> => {
  const result = (await esClient.search({
    index: TRAIN_INDEX,
    query: { match_all: {} },
    size: 100,
  })) as unknown as EsSimpleSearchResult<TrainDocument>;
  return result.hits.hits
    .map((h) => h._source)
    .filter((s): s is TrainDocument => s !== undefined);
};

export default {
  indexStation,
  indexTrainRoute,
  indexSchedule,
  cancelSchedule,
  updateSeatAvailability,
  searchTrains,
  autocompleteStation,
  getAllStations,
  getAllTrains,
};
