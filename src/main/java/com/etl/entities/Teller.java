package com.etl.entities;

import java.math.BigDecimal;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class Teller {
    @JsonProperty("VALUE_DATE_2")
    private Number valueDate2;

    @JsonProperty("CURRENCY_1")
    private String currency1;

    @JsonProperty("TRANSACTION_CODE")
    private String transactionCode;

    @JsonProperty("ID")
    private String id;

    @JsonProperty("AMOUNT_FCY_1")
    private Number amountFcy1;

    @JsonProperty("AMOUNT_FCY_2")
    private Number amountFcy2;

    @JsonProperty("RATE_2")
    private Number rate2;

    @JsonProperty("CUSTOMER_2")
    private String customer2;

    @JsonProperty("AUTHORISER")
    private String authoriser;

    @JsonProperty("OP_TYPE")
    private String opType;

    @JsonProperty("ACCOUNT_1")
    private String account1;

    @JsonProperty("ACCOUNT_2")
    private String account2;       // ACCOUNT_2
}
