package com.mycompany.taskmanagement.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FieldConfig {
    private String fieldId;
    private String displayName;
    private String fieldType;        // textbox | textarea | select
    private String defaultValue;
    private DropdownSource source;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DropdownSource {
        private String type;                 // static | http | file | entity
        private List<String> staticOptions;
        private String url;
        private String arrayName;
        private String nameAttribute;
        private String valueAttribute;
        private String fileName;             // classpath location of a key|value pipe-separated file, used when type = file
        private String entityType;           // project | programme, used when type = entity
    }
}
