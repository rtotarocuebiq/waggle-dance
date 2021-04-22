package com.hotels.bdp.waggledance.api.model;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;

import java.util.Arrays;
import java.util.List;

public class MappedDbs
{
    @NotBlank private String catalog;
    @NotEmpty private List<String> mappedDBs;

    public MappedDbs()
    {
    }

    public MappedDbs(@NotBlank String catalog, @NotEmpty List<String> mappedDBs)
    {
        this.catalog = catalog;
        this.mappedDBs = mappedDBs;
    }

    public MappedDbs(@NotBlank String catalog, @NotEmpty String ... mappedDBs)
    {
        this.catalog = catalog;
        this.mappedDBs = Arrays.asList(mappedDBs);
    }

    public String getCatalog()
    {
        return catalog;
    }

    public void setCatalog(String catalog)
    {
        this.catalog = catalog;
    }

    public List<String> getMappedDBs()
    {
        return mappedDBs;
    }

    public void setMappedDBs(List<String> mappedDBs)
    {
        this.mappedDBs = mappedDBs;
    }
}
