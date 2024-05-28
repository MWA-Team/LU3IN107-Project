package fr.su.memorydb.utils;

import fr.su.memorydb.database.Column;

import java.util.HashMap;

public abstract class ToolBox {

    public static HashMap<String, HashMap<Column, Integer>> columnsRepartition = new HashMap<>();

    private ToolBox() {}

}