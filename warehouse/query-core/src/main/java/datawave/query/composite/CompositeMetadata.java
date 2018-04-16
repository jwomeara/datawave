package datawave.query.composite;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import datawave.util.DateSchema;
import datawave.util.StringMultimapSchema;
import io.protostuff.Input;
import io.protostuff.LinkedBuffer;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.ProtobufIOUtil;
import io.protostuff.Schema;
import io.protostuff.StringMapSchema;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CompositeMetadata implements Message<CompositeMetadata> {
    
    private static final LinkedBuffer linkedBuffer;
    
    static {
        linkedBuffer = LinkedBuffer.allocate(4096);
    }
    
    protected Map<String,Multimap<String,String>> compositeFieldMapByType;
    protected Map<String,Map<String,Date>> compositeTransitionDatesByType;
    
    public CompositeMetadata() {
        this.compositeFieldMapByType = new HashMap<>();
        this.compositeTransitionDatesByType = new HashMap<>();
    }
    
    public Map<String,Multimap<String,String>> getCompositeFieldMapByType() {
        return compositeFieldMapByType;
    }
    
    public void setCompositeFieldMapByType(Map<String,Multimap<String,String>> compositeFieldMapByType) {
        this.compositeFieldMapByType = compositeFieldMapByType;
    }
    
    public void addCompositeFieldByType(String ingestType, String compositeField, Collection<String> componentFields) {
        Multimap<String,String> compositeFieldMap;
        if (!compositeFieldMapByType.containsKey(ingestType)) {
            compositeFieldMap = ArrayListMultimap.create();
            compositeFieldMapByType.put(ingestType, compositeFieldMap);
        } else
            compositeFieldMap = compositeFieldMapByType.get(ingestType);
        
        compositeFieldMap.putAll(compositeField, componentFields);
    }
    
    public Map<String,Map<String,Date>> getCompositeTransitionDatesByType() {
        return compositeTransitionDatesByType;
    }
    
    public void setCompositeTransitionDatesByType(Map<String,Map<String,Date>> compositeTransitionDatesByType) {
        this.compositeTransitionDatesByType = compositeTransitionDatesByType;
    }
    
    public void addCompositeTransitionDateByType(String ingestType, String compositeFieldName, Date transitionDate) {
        Map<String,Date> compositeTransitionDateMap;
        if (!compositeTransitionDatesByType.containsKey(ingestType)) {
            compositeTransitionDateMap = new HashMap<>();
            compositeTransitionDatesByType.put(ingestType, compositeTransitionDateMap);
        } else
            compositeTransitionDateMap = compositeTransitionDatesByType.get(ingestType);
        
        compositeTransitionDateMap.put(compositeFieldName, transitionDate);
    }
    
    public boolean isEmpty() {
        return (compositeFieldMapByType == null || compositeFieldMapByType.isEmpty())
                        && (compositeTransitionDatesByType == null || compositeTransitionDatesByType.isEmpty());
    }
    
    public CompositeMetadata filter(Set<String> compositeFields) {
        Set<String> ingestTypes = new HashSet<>();
        ingestTypes.addAll(this.compositeFieldMapByType.keySet());
        ingestTypes.addAll(this.compositeTransitionDatesByType.keySet());
        return filter(ingestTypes, compositeFields);
    }
    
    public CompositeMetadata filter(Set<String> ingestTypes, Set<String> compositeFields) {
        if (!isEmpty()) {
            CompositeMetadata compositeMetadata = new CompositeMetadata();
            for (String ingestType : ingestTypes) {
                for (String compositeField : compositeFields) {
                    if (this.compositeFieldMapByType.containsKey(ingestType) && this.compositeFieldMapByType.get(ingestType).containsKey(compositeField)) {
                        compositeMetadata.addCompositeFieldByType(ingestType, compositeField, this.compositeFieldMapByType.get(ingestType).get(compositeField));
                    }
                    
                    if (this.compositeTransitionDatesByType.containsKey(ingestType)
                                    && this.compositeTransitionDatesByType.get(ingestType).containsKey(compositeField)) {
                        compositeMetadata.addCompositeTransitionDateByType(ingestType, compositeField,
                                        this.compositeTransitionDatesByType.get(ingestType).get(compositeField));
                    }
                }
            }
            return compositeMetadata;
        }
        return this;
    }
    
    public static byte[] toBytes(CompositeMetadata compositeMetadata) {
        if (compositeMetadata != null && !compositeMetadata.isEmpty()) {
            byte[] bytes = ProtobufIOUtil.toByteArray(compositeMetadata, COMPOSITE_METADATA_SCHEMA, linkedBuffer);
            linkedBuffer.clear();
            return bytes;
        } else
            return new byte[] {};
    }
    
    public static CompositeMetadata fromBytes(byte[] compositeMetadataBytes) {
        CompositeMetadata compositeMetadata = COMPOSITE_METADATA_SCHEMA.newMessage();
        ProtobufIOUtil.mergeFrom(compositeMetadataBytes, compositeMetadata, COMPOSITE_METADATA_SCHEMA);
        return compositeMetadata;
    }
    
    @Override
    public Schema<CompositeMetadata> cachedSchema() {
        return COMPOSITE_METADATA_SCHEMA;
    }
    
    public static Schema<CompositeMetadata> COMPOSITE_METADATA_SCHEMA = new Schema<CompositeMetadata>() {
        
        public static final String COMPOSITE_FIELD_MAPPING_BY_TYPE = "compositeFieldMappingByType";
        public static final String COMPOSITE_TRANSITION_DATE_BY_TYPE = "compositeTransitionDatesByType";
        
        public Schema<Map<String,Multimap<String,String>>> compositeFieldMappingByTypeSchema = new StringMapSchema<>(new StringMultimapSchema());
        public Schema<Map<String,Map<String,Date>>> compositeTransitionDateByTypeSchema = new StringMapSchema<>(new StringMapSchema<>(new DateSchema()));
        
        @Override
        public String getFieldName(int number) {
            switch (number) {
                case 1:
                    return COMPOSITE_FIELD_MAPPING_BY_TYPE;
                case 2:
                    return COMPOSITE_TRANSITION_DATE_BY_TYPE;
                default:
                    return null;
            }
        }
        
        @Override
        public int getFieldNumber(String name) {
            switch (name) {
                case COMPOSITE_FIELD_MAPPING_BY_TYPE:
                    return 1;
                case COMPOSITE_TRANSITION_DATE_BY_TYPE:
                    return 2;
                default:
                    return 0;
            }
        }
        
        @Override
        public boolean isInitialized(CompositeMetadata compositeMetadata) {
            return true;
        }
        
        @Override
        public CompositeMetadata newMessage() {
            return new CompositeMetadata();
        }
        
        @Override
        public String messageName() {
            return CompositeMetadata.class.getSimpleName();
        }
        
        @Override
        public String messageFullName() {
            return CompositeMetadata.class.getName();
        }
        
        @Override
        public Class<? super CompositeMetadata> typeClass() {
            return CompositeMetadata.class;
        }
        
        @Override
        public void mergeFrom(Input input, CompositeMetadata compositeMetadata) throws IOException {
            for (int number = input.readFieldNumber(this);; number = input.readFieldNumber(this)) {
                switch (number) {
                    case 0:
                        return;
                    case 1:
                        compositeMetadata.setCompositeFieldMapByType(input.mergeObject(null, compositeFieldMappingByTypeSchema));
                        break;
                    case 2:
                        compositeMetadata.setCompositeTransitionDatesByType(input.mergeObject(null, compositeTransitionDateByTypeSchema));
                        break;
                    default:
                        input.handleUnknownField(number, this);
                }
            }
        }
        
        @Override
        public void writeTo(Output output, CompositeMetadata compositeMetadata) throws IOException {
            if (compositeMetadata.getCompositeFieldMapByType() != null)
                output.writeObject(1, compositeMetadata.getCompositeFieldMapByType(), compositeFieldMappingByTypeSchema, false);
            if (compositeMetadata.getCompositeTransitionDatesByType() != null)
                output.writeObject(2, compositeMetadata.getCompositeTransitionDatesByType(), compositeTransitionDateByTypeSchema, false);
        }
    };
}
