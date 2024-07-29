package org.opensearch.customdatasource.ossnapshot;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;

public class ValueConverters {
    public static List<BiFunction> getJsonConverters(StructType schema) {
        StructField[] fields = schema.fields();
        List<BiFunction> valueConverters = new ArrayList<>(fields.length);
        
        Arrays.stream(fields).forEach(field -> {
            if (field.dataType().equals(DataTypes.StringType)) {
                valueConverters.add(UTF8StringConverter);
            } else if (field.dataType().equals(DataTypes.IntegerType)) {
                valueConverters.add(IntConverter);
            } else if (field.dataType().equals(DataTypes.LongType)) {
                valueConverters.add(LongConverter);
            } else if (field.dataType().equals(DataTypes.DoubleType)) {
                valueConverters.add(DoubleConverter);
            } else if (field.dataType().equals(DataTypes.FloatType)) {
                valueConverters.add(FloatConverter);
            } else if (field.dataType().equals(DataTypes.BooleanType)) {
                valueConverters.add(BooleanConverter);
            } else if (field.dataType().equals(DataTypes.TimestampType)) {
                valueConverters.add(TimestampConverter);
            } else if (field.dataType() instanceof ArrayType) {
                ArrayType arrayType = (ArrayType) field.dataType();
                if (arrayType.elementType().equals(DataTypes.StringType)) {
                    valueConverters.add(StringArrayConverter);
                } else {
                    // Add more array type converters as needed
                    throw new UnsupportedOperationException("Unsupported array element type: " + arrayType.elementType());
                }
            } else {
                throw new UnsupportedOperationException("Unsupported data type: " + field.dataType());
            }
        });
        
        return valueConverters;
    }

    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");


    public static BiFunction UTF8StringConverter = (jsonObject, fieldName) -> {
        String value = ((JSONObject) jsonObject).optString((String) fieldName, null);
        return value != null ? UTF8String.fromString(value) : null;
    };

    public static BiFunction IntConverter = (jsonObject, fieldName) -> {
        JSONObject json = (JSONObject) jsonObject;
        String field = (String) fieldName;
        return json.has(field) ? json.getInt(field) : null;
    };

    public static BiFunction LongConverter = (jsonObject, fieldName) -> {
        JSONObject json = (JSONObject) jsonObject;
        String field = (String) fieldName;
        return json.has(field) ? json.getLong(field) : null;
    };

    public static BiFunction DoubleConverter = (jsonObject, fieldName) -> {
        JSONObject json = (JSONObject) jsonObject;
        String field = (String) fieldName;
        return json.has(field) ? json.getDouble(field) : null;
    };

    public static BiFunction BooleanConverter = (jsonObject, fieldName) -> {
        JSONObject json = (JSONObject) jsonObject;
        String field = (String) fieldName;
        return json.has(field) ? json.getBoolean(field) : null;
    };


    public static BiFunction TimestampConverter = (jsonObject, fieldName) -> {
        JSONObject json = (JSONObject) jsonObject;
        String field = (String) fieldName;
        if (json.has(field)) {
            String timestampStr = json.getString(field);
            LocalDateTime localDateTime = LocalDateTime.parse(timestampStr, TIMESTAMP_FORMATTER);
            return localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli() * 1000; // Convert to microseconds
        }
        return null;
    };

    public static BiFunction StringArrayConverter = (jsonObject, fieldName) -> {
        JSONObject json = (JSONObject) jsonObject;
        String field = (String) fieldName;
        if (json.has(field)) {
            JSONArray jsonArray = json.getJSONArray(field);
            List<UTF8String> result = new ArrayList<>(jsonArray.length());
            for (int i = 0; i < jsonArray.length(); i++) {
                result.add(UTF8String.fromString(jsonArray.getString(i)));
            }
            return result;
        }
        return null;
    };

    public static BiFunction FloatConverter = (jsonObject, fieldName) -> {
        JSONObject json = (JSONObject) jsonObject;
        String field = (String) fieldName;
        return json.has(field) ? (float) json.getDouble(field) : null;
    };
}