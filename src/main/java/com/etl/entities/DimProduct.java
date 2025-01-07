package com.etl.entities;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class DimProduct {
    private Long productNo;
    private String productCategory;
    private String productCode;
    private String productType;
    private String recordStat;
    private java.sql.Date effDt;
    private java.sql.Date endDt;
    private java.sql.Timestamp updateTms;
}