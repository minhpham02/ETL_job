package com.etl.entities;

import java.math.BigDecimal;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class Teller {
    private Number valueDate2;         // VALUE_DATE_2
    private String currency1;       // CURRENCY_1
    private String transactionCode; // TRANSACTION_CODE
    private String id;              // ID
    private Number amountFcy1;  // AMOUNT_FCY_1
    private Number amountFcy2;  // AMOUNT_FCY_2
    private Number rate2;           // RATE_2
    private String customer2;       // CUSTOMER_2
    private String authoriser;      // AUTHORISER
    private String opType;          // OP_TYPE
    private String account1;        // ACCOUNT_1
    private String account2;        // ACCOUNT_2
}
