package com.etl.entities;

import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Product {
    private Integer productNo;  
    private String locTerm;     
    private String category;          
    private String subProduct;      
    private String opType; 
}