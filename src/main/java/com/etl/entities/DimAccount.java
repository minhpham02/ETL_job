package com.etl.entities;

import java.sql.Timestamp;
import java.util.Date;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DimAccount {
    private String accountType;
    private String ccy;
    private String crGl;
    private String accountNo;
    private Date maturityDate;
    private Number rate;
    private String recordStat;
    private String accountClass;
    private Date effDt;
    private Date endDt;
    private Timestamp updateTms;
    private Number actF;
}
