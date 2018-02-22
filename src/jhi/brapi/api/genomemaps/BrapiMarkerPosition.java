package jhi.brapi.api.genomemaps;

/* temporary hack until JHI team upgrades to BrAPI v1.1 */

public class BrapiMarkerPosition {
    private String markerDbId;
    private String markerName;
    private String location;
    private String linkageGroupName;

    public String getMarkerDbId() {
        return this.markerDbId;
    }

    public void setMarkerDbId(String markerDbId) {
        this.markerDbId = markerDbId;
    }

    public String getMarkerName() {
        return this.markerName;
    }

    public void setMarkerName(String markerName) {
        this.markerName = markerName;
    }

    public String getLocation() {
        return this.location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getLinkageGroupName() {
        return this.linkageGroupName;
    }

    public void setLinkageGroupName(String linkageGroupName) {
        this.linkageGroupName = linkageGroupName;
    }
}

