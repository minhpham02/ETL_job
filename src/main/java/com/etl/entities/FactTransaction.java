package com.etl.entities;

import java.sql.Date;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class FactTransaction {
    private String txnCcy;       // TXN_CCY
    private String productCode;  // PRODUCT_CODE
    private String trnRefNo;     // TRN_REF_NO
    private double lcyAmount;    // LCY_AMOUNT
    private double txnAmount;    // TXN_AMOUNT
    private String cstDimId;     // CST_DIM_ID
    private String recordStat;   // RECORD_STAT
    private String authStat;     // AUTH_STAT
    private String txnAcc;       // TXN_ACC
    private String ofsAcc;       // OFS_ACC
    private Date trnDt;          // TRN_DT
    private Date etlDate;        // ETL DATE
}
