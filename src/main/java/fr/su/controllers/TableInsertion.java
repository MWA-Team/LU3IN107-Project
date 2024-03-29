package fr.su.controllers;

import fr.su.DownloadFormData;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

@Path("insert")
public class TableInsertion {

    @POST
    @Consumes(MediaType.TEXT_PLAIN)
    @Path("file")
    public String getFile(InputStream inputStream) throws IOException {

        return IOUtils.toString(inputStream, StandardCharsets.UTF_8);

    }
}
