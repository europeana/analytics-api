package eu.europeana.api.analytics.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Registered Clients  class.
 * Hold the projects and internal clients keys
 * @author srishti singh
 */
public class RegisteredClients {

    @JsonProperty("projects")
    List<String> projects;

    @JsonProperty("internal")
    List<String> internal;

    public List<String> getProjects() {
        return projects;
    }

    public void setProjects(List<String> projects) {
        this.projects = projects;
    }

    public List<String> getInternal() {
        return internal;
    }

    public void setInternal(List<String> internal) {
        this.internal = internal;
    }
}
