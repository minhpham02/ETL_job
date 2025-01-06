package com.etl.entities;

import java.sql.Date;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class Product {
    private Number productNo;           // PRODUCT_NO
    private String locTerm;     // PRODUCT_CATEGORY
    private String category;         // PRODUCT_CODE
    private String subProduct;         // PRODUCT_TYPE
    private String opType;        // RECORD_STAT
    private Date effectiveDate;         // EFF_DT
    private Date endDate;               // END_DT
    private java.sql.Timestamp updateTimestamp; // UPDATE_TMS
}
