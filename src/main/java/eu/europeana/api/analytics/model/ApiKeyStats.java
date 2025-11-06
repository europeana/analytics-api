package eu.europeana.api.analytics.model;

import java.io.Serial;
import java.util.HashMap;

/**
 * Apikey statistics class.
 * Hold the statistics fetched from the ELK
 * @author srishti singh
 */
public class ApiKeyStats extends HashMap<String,Integer> {

    @Serial
    private static final long serialVersionUID = -5270775130270603183L;

    private String apikey;
    private int    total;
    private int    active;

    /**
     * Default constructor
     * @param apikey apikey of the statistics
     */
    public ApiKeyStats(String apikey) { this.apikey = apikey; }

    public String getApiKey()           { return apikey;   }
    public int    getTotal()            { return total;    }
    public void   setTotal(int total)   { this.total = total;  }
    public int    getActive()           { return active;   }
    public void   setActive(int active) { this.active = active; }

    @Override
    public String toString() {
        return getApiKey();
    }
}
