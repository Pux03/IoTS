using System;
using System.Collections.Generic;
using Microsoft.EntityFrameworkCore;

namespace rest_service.Models;

public partial class AccessControlSystemContext : DbContext
{
    public AccessControlSystemContext()
    {
    }

    public AccessControlSystemContext(DbContextOptions<AccessControlSystemContext> options)
        : base(options)
    {
    }

    public virtual DbSet<Event> Events { get; set; }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {

    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Event>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("events_pkey");

            entity.ToTable("events");

            entity.HasIndex(e => e.DeviceId, "idx_events_device_id");

            entity.HasIndex(e => new { e.DeviceId, e.Timestamp }, "idx_events_device_timestamp").IsDescending(false, true);

            entity.HasIndex(e => e.Timestamp, "idx_events_timestamp").IsDescending();

            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.AccessGranted).HasColumnName("access_granted");
            entity.Property(e => e.BatteryVoltage).HasColumnName("battery_voltage");
            entity.Property(e => e.CardUid)
                .HasMaxLength(32)
                .HasColumnName("card_uid");
            entity.Property(e => e.DeviceId)
                .HasMaxLength(32)
                .HasColumnName("device_id");
            entity.Property(e => e.DoorId)
                .HasMaxLength(32)
                .HasColumnName("door_id");
            entity.Property(e => e.EventId).HasColumnName("event_id");
            entity.Property(e => e.EventType)
                .HasMaxLength(32)
                .HasColumnName("event_type");
            entity.Property(e => e.ResponseTimeMs).HasColumnName("response_time_ms");
            entity.Property(e => e.SignalStrength).HasColumnName("signal_strength");
            entity.Property(e => e.Temperature).HasColumnName("temperature");
            entity.Property(e => e.Timestamp)
                .HasColumnType("timestamp without time zone")
                .HasColumnName("timestamp");
            entity.Property(e => e.Zone)
                .HasMaxLength(32)
                .HasColumnName("zone");
        });

        OnModelCreatingPartial(modelBuilder);
    }

    partial void OnModelCreatingPartial(ModelBuilder modelBuilder);
}
