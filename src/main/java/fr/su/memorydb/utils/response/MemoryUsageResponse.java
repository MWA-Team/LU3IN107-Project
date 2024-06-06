package fr.su.memorydb.utils.response;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;

public class MemoryUsageResponse extends ValidResponse {

    @JsonProperty
    HashMap<String, Double> memoryUsage;

    public MemoryUsageResponse(HashMap<String, Double> memoryUsage) {
        super();
        this.memoryUsage = memoryUsage;
    }

    public HashMap<String, Double> getMemoryUsage() {
        return memoryUsage;
    }

}
