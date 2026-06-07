using System;
using System.Collections.Generic;

namespace rest_service.Models;

public partial class Event
{
    public long Id { get; set; }

    public Guid EventId { get; set; }

    public DateTime Timestamp { get; set; }

    public string DeviceId { get; set; } = null!;

    public string CardUid { get; set; } = null!;

    public bool AccessGranted { get; set; }

    public string DoorId { get; set; } = null!;

    public string Zone { get; set; } = null!;

    public int SignalStrength { get; set; }

    public float BatteryVoltage { get; set; }

    public int ResponseTimeMs { get; set; }

    public string EventType { get; set; } = null!;

    public float Temperature { get; set; }
}
