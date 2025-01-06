package com.etl.entities;

import java.sql.Date;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Account {
    private String id;
    private String customer;
    private String prtCode;
    private String currency; //VND, USD
    private Long openValDateBal;
    private Date dateMaturity;
    private String opType; //D or other char (abcd)
    private Long workingBalance;
    private String allInOneProduct;
    private String category; //1008, 1005
    private String coCode; //001, 002
}
