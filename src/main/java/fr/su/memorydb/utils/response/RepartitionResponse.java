package fr.su.memorydb.utils.response;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;

public class RepartitionResponse extends ValidResponse {

    @JsonProperty
    HashMap<String, HashMap<String, String>> tables;

    public RepartitionResponse(HashMap<String, HashMap<String, String>> tables) {
        super();
        this.tables = tables;
    }

    public HashMap<String, HashMap<String, String>> getTables() {
        return tables;
    }

}
