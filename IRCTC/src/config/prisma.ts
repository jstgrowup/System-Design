import { PrismaClient } from '@prisma/client';
import { PrismaPg } from '@prisma/adapter-pg';
import pg from 'pg';
import { config } from './index.js';

// Define a type extending the Node global object to handle our cached client safely
interface CustomGlobal extends typeof globalThis {
    prisma?: PrismaClient;
}

const globalForPrisma = global as CustomGlobal;

let prisma: PrismaClient;

if (!globalForPrisma.prisma) {
    // 1. Establish a PostgreSQL connection pool via the 'pg' library
    const pool = new pg.Pool({ connectionString: config.DATABASE_URL });
    
    // 2. Instantiate the Prisma PostgreSQL driver adapter with the pool instance
    const adapter = new PrismaPg(pool);

    // 3. Create the centralized Client instance
    globalForPrisma.prisma = new PrismaClient({
        adapter,
        log: ['error', 'warn'],
    });
}

prisma = globalForPrisma.prisma;

export default prisma;