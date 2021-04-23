package com.hotels.bdp.waggledance.util;

import java.util.List;
import java.util.stream.Collectors;

public class CatalogUtils
{
    public static String createDbNameWithCatalog(String catalog, String databaseName)
    {
        if(catalog!=null) {
            return "@".concat(catalog).concat("#").concat(databaseName);
        }
        else
        {
            return databaseName;
        }
    }

    public static String cleanDbName(String databaseName)
    {
        if(databaseName.startsWith("@"))
        {
            return databaseName.substring(1).split("#")[1];
        }
        return databaseName;
    }

    public static List<String> createDbNamesWithCatalog(String catalog,List<String> databases)
    {
        return databases.stream().map(s -> createDbNameWithCatalog(catalog,s)).collect(Collectors.toList());
    }
}

