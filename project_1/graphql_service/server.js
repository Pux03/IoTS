const express = require("express");
const { ApolloServer, gql } = require("apollo-server-express");
const { Sequelize, DataTypes } = require("sequelize");

// 1. GraphQL šema
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

  type Query {
    getEvents: [Event]
    getEvent(id: ID!): Event
  }
`;

// 2. Konekcija na bazu (izmeni kredencijale po potrebi)
const sequelize = new Sequelize("access_control_system", "admin", "admin", {
  host: "iot-access-control-system-postgres", // Ime tvog postgres docker kontejnera
  dialect: "postgres",
  logging: false,
});

// 3. Model koji mapira postojeću "events" tablicu
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
    tableName: "events", // Tvoja tabela iz baze
    timestamps: false, // Isključujemo defaultne Sequelize createdAt/updatedAt kolone
  },
);

// Pomoćna funkcija koja analizira GraphQL upit i izvlači samo tražena polja
function getSelectedFields(info) {
  return info.fieldNodes[0].selectionSet.selections.map(
    (selection) => selection.name.value,
  );
}

// 4. GraphQL Resolveri
const resolvers = {
  Query: {
    getEvents: async (_, __, context, info) => {
      const attributes = getSelectedFields(info); // Izvlači npr: ['id', 'timestamp']
      return await EventModel.findAll({ attributes }); // SELECT id, timestamp FROM events;
    },
    getEvent: async (_, { id }, context, info) => {
      const attributes = getSelectedFields(info);
      return await EventModel.findByPk(id, { attributes });
    },
  },
};

async function startServer() {
  const app = express();
  const server = new ApolloServer({ typeDefs, resolvers });

  await server.start();
  server.applyMiddleware({ app, path: "/graphql" });

  const PORT = 4000;
  app.listen(PORT, () => {
    console.log(
      `🚀 GraphQL servis spreman na http://localhost:${PORT}/graphql`,
    );
  });
}

startServer().catch((err) => console.error("Greška pri pokretanju:", err));
