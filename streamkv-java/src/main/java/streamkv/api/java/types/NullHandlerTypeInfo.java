package streamkv.api.java.types;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

public class NullHandlerTypeInfo<T> extends TypeInformation<T> {

	private static final long serialVersionUID = 1L;
	private TypeInformation<T> typeInfo;

	public NullHandlerTypeInfo(TypeInformation<T> typeInfo) {
		super();
		this.typeInfo = typeInfo;
	}

	@Override
	public boolean isBasicType() {
		return typeInfo.isBasicType();
	}

	@Override
	public boolean isTupleType() {
		return typeInfo.isTupleType();
	}

	@Override
	public int getArity() {
		return typeInfo.getArity();
	}

	@Override
	public int getTotalFields() {
		return typeInfo.getTotalFields();
	}

	@Override
	public Class<T> getTypeClass() {
		return typeInfo.getTypeClass();
	}

	@Override
	public boolean isKeyType() {
		return typeInfo.isKeyType();
	}

	@Override
	public TypeSerializer<T> createSerializer(ExecutionConfig config) {
		return new NullHandlerSerializer<>(typeInfo.createSerializer(config));
	}

	@Override
	public String toString() {
		return "NullHandler(" + typeInfo.toString() + ")";
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof NullHandlerTypeInfo;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((typeInfo == null) ? 0 : typeInfo.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		return typeInfo.equals(obj);
	}

}
