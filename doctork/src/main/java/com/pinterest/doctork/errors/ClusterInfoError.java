package com.pinterest.doctork.errors;

import java.lang.Exception;

public class ClusterInfoError extends Exception {
    String[] errors;

    public ClusterInfoError() {
        this.errors = new String[1];
	this.errors[0] = "Unknown error";
    }

    public ClusterInfoError(String... errors) {
        this.errors = new String[errors.length];
        int i = 0;
        for(String error : errors ) {
            this.errors[i++] = error;
        }
    }
}
