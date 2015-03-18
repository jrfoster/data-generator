package org.jrfoster.datagen;

import org.apache.commons.lang3.StringUtils;

/**
 * This class holds data related to a zip code, such as the city and state, 
 * its geographic location (lat/lon), estimated population, etc.
 * 
 * @author jasonf
 *
 */
public class ZipData {
    private String zipCode;
    private String zipCodeType;
    private String city;
    private String state;
    private String locationType;
    private Double latitude;
    private Double longitude;
    private String location;
    private boolean decommisioned;
    private Integer taxReturnsFiled;
    private Integer estimatedPopulation;
    private Double totalWages;

    public ZipData(String input) {
        if (input == null || input.isEmpty()) {
            throw new IllegalArgumentException("input cannot be null");
        }

        String[] tokens = input.split(",");

        if (tokens.length < 9) {
            throw new IllegalArgumentException("input not formatted correctly");
        }

        zipCode = StringUtils.leftPad(tokens[0], 5, '0');
        zipCodeType = tokens[1];
        city = tokens[2];
        state = tokens[3];
        locationType = tokens[4];
        latitude = tokens[5] != null && !tokens[5].isEmpty() ? Double
                .valueOf(tokens[5]) : null;
        longitude = tokens[6] != null && !tokens[6].isEmpty() ? Double
                .valueOf(tokens[6]) : null;
        location = tokens[7];
        decommisioned = tokens[8] != null && !tokens[8].isEmpty() ? Boolean
                .valueOf(tokens[8]) : null;
        // taxReturnsFiled = tokens[9] != null ? Integer.valueOf(tokens[9]) :
        // null;
        // estimatedPopulation = tokens[10] != null ?
        // Integer.valueOf(tokens[10]) : null;
        // totalWages = tokens[11] != null ? Double.valueOf(tokens[11]) : null;
    }

    public String getZipCode() {
        return zipCode;
    }

    public String getZipCodeType() {
        return zipCodeType;
    }

    public String getCity() {
        return city;
    }

    public String getState() {
        return state;
    }

    public String getLocationType() {
        return locationType;
    }

    public Double getLatitude() {
        return latitude;
    }

    public Double getLongitude() {
        return longitude;
    }

    public String getLocation() {
        return location;
    }

    public boolean isDecommisioned() {
        return decommisioned;
    }

    public Integer getTaxReturnsFiled() {
        return taxReturnsFiled;
    }

    public Integer getEstimatedPopulation() {
        return estimatedPopulation;
    }

    public Double getTotalWages() {
        return totalWages;
    }

}
