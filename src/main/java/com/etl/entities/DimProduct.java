package com.etl.entities;

import java.sql.Timestamp;
import java.util.Date;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DimProduct {
    private int productNo;           
    private String productCategory;  
    private String productCode;      
    private String productType;      
    private String recordStat;       
    private Date effDt;              
    private Date endDt;              
    private Timestamp updateTms; 
}