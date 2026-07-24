package com.mycompany.taskmanagement.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DropdownSourceConfig {
    private String fieldName;
    private String url;
    private String arrayName;
    private String nameAttribute;
    private String valueAttribute;
}
