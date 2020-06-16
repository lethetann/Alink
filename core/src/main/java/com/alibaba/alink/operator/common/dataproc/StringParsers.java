package com.alibaba.alink.operator.common.dataproc;

import com.alibaba.alink.common.utils.JsonConverter;
import com.jayway.jsonpath.JsonPath;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

public class StringParsers {

    interface StringParser {
        /**
         * Parse a text line to a `Row`. If some of fields are missing or mal-formatted, then null is padded.
         *
         * @param line The line to parse.
         * @return The resulted Row. Tuple.f0 indicates whether we successfully parse all fields, Tuple.f1 is the parsed result.
         */
        Tuple2<Boolean, Row> parse(String line);
    }

    /**
     * JsonParser parse a json string to a {@link Row}.
     */
    public static class JsonParser implements StringParser {
        private String[] fieldNames;
        private FieldParser<?>[] parsers;
        private boolean[] isString;

        public JsonParser(String[] fieldNames, TypeInformation[] fieldTypes) {
            this.fieldNames = fieldNames;
            Preconditions.checkArgument(fieldNames.length == fieldTypes.length);
            this.isString = new boolean[fieldNames.length];
            this.parsers = new FieldParser[fieldNames.length];

            for (int i = 0; i < fieldTypes.length; i++) {
                parsers[i] = getFieldParser(fieldTypes[i].getTypeClass());
                isString[i] = fieldTypes[i].equals(Types.STRING);
            }
        }

        @Override
        public Tuple2<Boolean, Row> parse(String line) {
            Row row = new Row(fieldNames.length);
            boolean succ = true;
            for (int i = 0; i < fieldNames.length; i++) {
                Object o = JsonPath.read(line, "$." + fieldNames[i]);
                if (o == null) {
                    succ = false;
                    continue;
                }
                if (!(o instanceof String)) {
                    o = JsonConverter.toJson(o);
                }
                Tuple2<Boolean, Object> parsed = parseField(parsers[i], (String) o, isString[i]);
                if (!parsed.f0) {
                    succ = false;
                }
                row.setField(i, parsed.f1);
            }
            return Tuple2.of(succ, row);
        }


    }

    /**
     * KvParser parse a key-value string to a {@link Row}.
     */
    public static class KvParser implements StringParser {
        private String[] fieldNames;
        private FieldParser<?>[] parsers;
        private boolean[] isString;
        private String colDelimiter;
        private String valDelimiter;
        private transient Map<String, Integer> keyToFieldIdx;

        public KvParser(String[] fieldNames, TypeInformation[] fieldTypes, String colDelimiter, String valDelimiter) {
            this.fieldNames = fieldNames;
            Preconditions.checkArgument(fieldNames.length == fieldTypes.length);
            this.isString = new boolean[fieldNames.length];
            this.parsers = new FieldParser[fieldNames.length];

            for (int i = 0; i < fieldTypes.length; i++) {
                parsers[i] = getFieldParser(fieldTypes[i].getTypeClass());
                isString[i] = fieldTypes[i].equals(Types.STRING);
            }
            this.colDelimiter = colDelimiter;
            this.valDelimiter = valDelimiter;

            keyToFieldIdx = new HashMap<>();
            for (int i = 0; i < fieldNames.length; i++) {
                keyToFieldIdx.put(fieldNames[i], i);
            }
        }

        @Override
        public Tuple2<Boolean, Row> parse(String line) {
            Row row = new Row(fieldNames.length);
            String[] fields = line.split(colDelimiter);
            boolean succ = true;
            int cnt = 0;

            for (int i = 0; i < fields.length; i++) {
                if (StringUtils.isNullOrWhitespaceOnly(fields[i])) {
                    succ = false;
                    continue;
                }
                String[] kv = fields[i].split(valDelimiter);
                if (kv.length < 2) {
                    succ = false;
                    continue;
                }
                Integer fidx = keyToFieldIdx.get(kv[0]);
                if (fidx == null) {
                    continue;
                }
                Tuple2<Boolean, Object> parsed = parseField(parsers[i], kv[1], isString[i]);
                if (!parsed.f0) {
                    succ = false;
                }
                row.setField(i, parsed.f1);
                cnt++;
            }

            if (cnt < fieldNames.length) {
                succ = false;
            }
            return Tuple2.of(succ, row);
        }
    }

    /**
     * CsvParser parse a CSV formatted text line to a {@link Row}.
     */
    public static class CsvParser implements StringParser {
        com.alibaba.alink.operator.common.io.csv.CsvParser parser;

        public CsvParser(TypeInformation[] types, String fieldDelim, @Nullable Character quoteChar) {
            this.parser = new com.alibaba.alink.operator.common.io.csv.CsvParser(types, fieldDelim, quoteChar);
        }

        @Override
        public Tuple2<Boolean, Row> parse(String line) {
            return parser.parse(line);
        }
    }

    static FieldParser<?> getFieldParser(Class typeClazz) {
        Class<? extends FieldParser<?>> parserType = FieldParser.getParserForType(typeClazz);
        if (parserType == null) {
            throw new RuntimeException("No parser available for type '" + typeClazz.getName() + "'.");
        }
        return InstantiationUtil.instantiate(parserType, FieldParser.class);
    }

    static Tuple2<Boolean, Object> parseField(FieldParser<?> parser, String token, boolean isStringField) {
        if (isStringField) {
            return Tuple2.of(true, token);
        } else {
            if (StringUtils.isNullOrWhitespaceOnly(token)) {
                return Tuple2.of(false, null);
            }
            byte[] bytes = token.getBytes();
            parser.resetErrorStateAndParse(bytes, 0, bytes.length, new byte[]{0}, null);
            FieldParser.ParseErrorState errorState = parser.getErrorState();
            if (errorState != FieldParser.ParseErrorState.NONE) {
                return Tuple2.of(false, null);
            } else {
                return Tuple2.of(true, parser.getLastResult());
            }
        }
    }
}
