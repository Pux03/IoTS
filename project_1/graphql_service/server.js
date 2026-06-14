const express = require("express");
const { ApolloServer, gql } = require("apollo-server-express");
const { Sequelize, DataTypes, Op } = require("sequelize");

const typeDefs = gql`
  type Event {
    id: ID!
    eventId: String
    timestamp: String
    deviceId: String
    cardUid: String
    accessGranted: Boolean
    doorId: String
    zone: String
    signalStrength: Int
    batteryVoltage: Float
    responseTimeMs: Int
    eventType: String
    temperature: Float
  }

  type SelectiveEvent {
    deviceId: String
    cardUid: String
  }

  type HeavyQueryResult {
    deviceId: String
    eventType: String
    zone: String
    count: Int
    averageResponseTimeMs: Float
    averageBatteryVoltage: Float
    minimumTemperature: Float
    maximumTemperature: Float
    firstTimestamp: String
    lastTimestamp: String
  }

  input EventInput {
    eventId: String
    timestamp: String
    deviceId: String!
    cardUid: String!
    accessGranted: Boolean!
    doorId: String!
    zone: String!
    signalStrength: Int!
    batteryVoltage: Float!
    responseTimeMs: Int!
    eventType: String!
    temperature: Float!
  }

  type Query {
    events(page: Int = 1, pageSize: Int = 50, deviceId: String): [Event!]!
    event(id: ID!): Event
    selectiveEvents(page: Int = 1, pageSize: Int = 50): [SelectiveEvent!]!
    heavyEvents(
      deviceId: String
      cardUid: String
      eventType: String
      fromDate: String
      toDate: String
      searchTerm: String
      pageSize: Int = 50
    ): [HeavyQueryResult!]!
  }

  type Mutation {
    createEvent(input: EventInput!): Event!
  }
`;

const sequelize = new Sequelize(
  process.env.DB_NAME || "access_control_system",
  process.env.DB_USER || "admin",
  process.env.DB_PASSWORD || "admin",
  {
    host: process.env.DB_HOST || "iot-access-control-system-postgres",
    port: Number(process.env.DB_PORT || 5432),
    dialect: "postgres",
    logging: false,
  },
);

const EventModel = sequelize.define(
  "Event",
  {
    id: { type: DataTypes.BIGINT, primaryKey: true, autoIncrement: true },
    eventId: { type: DataTypes.UUID, field: "event_id" },
    timestamp: { type: DataTypes.DATE },
    deviceId: { type: DataTypes.STRING, field: "device_id" },
    cardUid: { type: DataTypes.STRING, field: "card_uid" },
    accessGranted: { type: DataTypes.BOOLEAN, field: "access_granted" },
    doorId: { type: DataTypes.STRING, field: "door_id" },
    zone: { type: DataTypes.STRING },
    signalStrength: { type: DataTypes.INTEGER, field: "signal_strength" },
    batteryVoltage: { type: DataTypes.FLOAT, field: "battery_voltage" },
    responseTimeMs: { type: DataTypes.INTEGER, field: "response_time_ms" },
    eventType: { type: DataTypes.STRING, field: "event_type" },
    temperature: { type: DataTypes.FLOAT },
  },
  {
    tableName: "events",
    timestamps: false,
  },
);

const fieldToColumn = {
  id: "id",
  eventId: "eventId",
  timestamp: "timestamp",
  deviceId: "deviceId",
  cardUid: "cardUid",
  accessGranted: "accessGranted",
  doorId: "doorId",
  zone: "zone",
  signalStrength: "signalStrength",
  batteryVoltage: "batteryVoltage",
  responseTimeMs: "responseTimeMs",
  eventType: "eventType",
  temperature: "temperature",
};

function clamp(value, fallback, min, max) {
  const parsed = Number(value || fallback);
  return Math.max(min, Math.min(max, parsed));
}

function getSelectedFields(info) {
  return info.fieldNodes[0].selectionSet.selections
    .map((selection) => fieldToColumn[selection.name.value])
    .filter(Boolean);
}

function buildWhere(args) {
  const where = {};

  if (args.deviceId) where.deviceId = args.deviceId;
  if (args.cardUid) where.cardUid = args.cardUid;
  if (args.eventType) where.eventType = args.eventType;

  if (args.fromDate || args.toDate) {
    where.timestamp = {};
    if (args.fromDate) where.timestamp[Op.gte] = new Date(args.fromDate);
    if (args.toDate) where.timestamp[Op.lte] = new Date(args.toDate);
  }

  if (args.searchTerm) {
    where[Op.or] = [
      { zone: { [Op.iLike]: `%${args.searchTerm}%` } },
      { doorId: { [Op.iLike]: `%${args.searchTerm}%` } },
    ];
  }

  return where;
}

const resolvers = {
  Query: {
    events: async (_, args, __, info) => {
      const page = clamp(args.page, 1, 1, 100000);
      const pageSize = clamp(args.pageSize, 50, 1, 100);
      const attributes = getSelectedFields(info);

      return EventModel.findAll({
        attributes,
        where: buildWhere(args),
        order: [["timestamp", "DESC"]],
        offset: (page - 1) * pageSize,
        limit: pageSize,
      });
    },
    event: async (_, { id }, __, info) => {
      return EventModel.findByPk(id, { attributes: getSelectedFields(info) });
    },
    selectiveEvents: async (_, args) => {
      const page = clamp(args.page, 1, 1, 100000);
      const pageSize = clamp(args.pageSize, 50, 1, 100);

      return EventModel.findAll({
        attributes: ["deviceId", "cardUid"],
        order: [["timestamp", "DESC"]],
        offset: (page - 1) * pageSize,
        limit: pageSize,
      });
    },
    heavyEvents: async (_, args) => {
      const pageSize = clamp(args.pageSize, 50, 1, 100);

      const rows = await EventModel.findAll({
        attributes: [
          "deviceId",
          "eventType",
          "zone",
          [sequelize.fn("COUNT", sequelize.col("id")), "count"],
          [sequelize.fn("AVG", sequelize.col("response_time_ms")), "averageResponseTimeMs"],
          [sequelize.fn("AVG", sequelize.col("battery_voltage")), "averageBatteryVoltage"],
          [sequelize.fn("MIN", sequelize.col("temperature")), "minimumTemperature"],
          [sequelize.fn("MAX", sequelize.col("temperature")), "maximumTemperature"],
          [sequelize.fn("MIN", sequelize.col("timestamp")), "firstTimestamp"],
          [sequelize.fn("MAX", sequelize.col("timestamp")), "lastTimestamp"],
        ],
        where: buildWhere(args),
        group: ["deviceId", "eventType", "zone"],
        order: [[sequelize.literal("count"), "DESC"]],
        limit: pageSize,
        raw: true,
      });

      return rows.map((row) => ({
        ...row,
        count: Number(row.count),
        averageResponseTimeMs: Number(row.averageResponseTimeMs),
        averageBatteryVoltage: Number(row.averageBatteryVoltage),
      }));
    },
  },
  Mutation: {
    createEvent: async (_, { input }) => {
      return EventModel.create({
        ...input,
        eventId: input.eventId || undefined,
        timestamp: input.timestamp ? new Date(input.timestamp) : new Date(),
      });
    },
  },
};

async function startServer() {
  await sequelize.authenticate();

  const app = express();
  const server = new ApolloServer({ typeDefs, resolvers });

  await server.start();
  server.applyMiddleware({ app, path: "/graphql" });

  const port = Number(process.env.PORT || 4000);
  app.listen(port, () => {
    console.log(`GraphQL service ready at http://localhost:${port}/graphql`);
  });
}

startServer().catch((err) => {
  console.error("Failed to start GraphQL service:", err);
  process.exit(1);
});
