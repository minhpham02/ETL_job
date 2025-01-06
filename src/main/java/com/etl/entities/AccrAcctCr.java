package com.etl.entities;

import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AccrAcctCr {
    private String accountNumber;
    private Number crIntRate;
}