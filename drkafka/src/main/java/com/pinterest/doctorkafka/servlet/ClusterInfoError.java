package com.pinterest.doctorkafka.servlet;

import java.util.ArrayList;
import java.util.List;

public class ClusterInfoError {
    String[] errors;

    public ClusterInfoError(String... errors) {
        this.errors = new String[errors.length];
        int i = 0;
        for(String error : errors ) {
            this.errors[i++] = error;
        }
    }
}
