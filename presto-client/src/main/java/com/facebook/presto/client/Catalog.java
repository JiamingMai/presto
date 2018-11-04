package com.facebook.presto.client;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Map;

public class Catalog
{
    private String catalogName;

    private String connectorName;

    private boolean updateCatalog;

    private Map<String, String> properties;

    @JsonProperty
    public String getCatalogName()
    {
        return catalogName;
    }

    @JsonProperty
    public void setCatalogName(String catalogName)
    {
        this.catalogName = catalogName;
    }

    @JsonProperty
    public String getConnectorName()
    {
        return connectorName;
    }

    @JsonProperty
    public void setConnectorName(String connectorName)
    {
        this.connectorName = connectorName;
    }

    @JsonProperty
    public boolean isUpdateCatalog()
    {
        return updateCatalog;
    }

    @JsonProperty
    public void setUpdateCatalog(boolean updateCatalog)
    {
        this.updateCatalog = updateCatalog;
    }

    @JsonProperty
    public Map<String, String> getProperties()
    {
        return properties;
    }

    @JsonProperty
    public void setProperties(Map<String, String> properties)
    {
        this.properties = properties;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder(this)
                .append("catalogName", catalogName)
                .append("connectorName", connectorName)
                .append("updateCatalog", updateCatalog)
                .append("properties", properties)
                .toString();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        Catalog rhs = (Catalog) obj;
        return new EqualsBuilder()
                .append(this.catalogName, rhs.catalogName)
                .append(this.connectorName, rhs.connectorName)
                .append(this.properties, rhs.properties)
                .isEquals();
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder()
                .append(catalogName)
                .append(connectorName)
                .append(properties)
                .toHashCode();
    }
}
