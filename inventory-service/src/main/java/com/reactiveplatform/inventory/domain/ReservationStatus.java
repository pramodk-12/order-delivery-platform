package com.reactiveplatform.inventory.domain;

public enum ReservationStatus {
    RESERVED,    // stock held, order not paid yet
    CONFIRMED,   // payment succeeded: reserved stock is now sold
    RELEASED     // payment failed / order cancelled: stock returned (saga compensation)
}
