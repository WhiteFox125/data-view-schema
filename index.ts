export const TypeSize = {
  Float16: 2,
  Float32: 4,
  Float64: 8,
  Int8: 1,
  Int16: 2,
  Int32: 4,
  Uint8: 1,
  Uint16: 2,
  Uint32: 4,
  BigInt64: 8,
  BigUint64: 8,
} as const;

export type TypeSizeKey = keyof typeof TypeSize;
export type ValueType<K extends TypeSizeKey> = K extends "BigInt64" | "BigUint64" ? bigint : number;

export type DataViewBackend = {
  readonly buffer: ArrayBufferLike;
  readonly byteOffset: number;
  readonly byteLength: number;
} & {
  [K in TypeSizeKey as `get${K}`]: (byteOffset: number, littleEndian?: boolean) => ValueType<K>;
} & {
  [K in TypeSizeKey as `set${K}`]: (
    byteOffset: number,
    value: ValueType<K>,
    littleEndian?: boolean
  ) => void;
};

export interface FieldDefinition<T> {
  readonly byteLength: number;
  readonly offset: number;
  read(backend: DataViewBackend, baseOffset: number): T;
  write(backend: DataViewBackend, baseOffset: number, value: T): void;
}

export interface ProxyCallbacks<Fields extends Record<string, FieldDefinition<any>>> {
  beforeRead?: (fieldName: keyof Fields) => void;
  afterRead?: (fieldName: keyof Fields, value: any) => void;
  beforeWrite?: (fieldName: keyof Fields, value: any) => void;
  afterWrite?: (fieldName: keyof Fields) => void;
}

function getByteLengthForInteger(value: number): number {
  if (value < 0 || !Number.isSafeInteger(value)) {
    throw new Error("Value must be a non-negative safe integer");
  }
  if (value === 0) return 1;

  let size = 0;
  do {
    size++;
    value = Math.floor(value / 256);
  } while (value > 0);
  return size;
}

export function bufferToDataView(buffer: Buffer) {
  return new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
}

export function dataViewToBuffer(dataView: DataView) {
  return Buffer.from(dataView.buffer, dataView.byteOffset, dataView.byteLength);
}

export default class DataViewSchema<Fields extends Record<string, FieldDefinition<any>> = {}> {
  private offset = 0;
  private readonly fields: { [K in keyof Fields]: FieldDefinition<any> } = {} as Fields;

  private registerField<FieldName extends string, T, Field extends FieldDefinition<T>>(
    fieldName: FieldName,
    field: Field
  ): DataViewSchema<Fields & Record<FieldName, Field>> {
    if (fieldName in this.fields) {
      throw new Error(`Field name '${fieldName}' is already registered`);
    }

    this.fields[fieldName] = field;
    this.offset += field.byteLength;

    return this as DataViewSchema<Fields & Record<FieldName, Field>>;
  }

  private createField<Type extends TypeSizeKey>(type: Type) {
    return <T extends ValueType<Type>, FieldName extends string>(
      fieldName: FieldName,
      littleEndian: boolean = false,
      allowedValues?: readonly T[]
    ) => {
      const byteLength = TypeSize[type];
      const field: FieldDefinition<T> = {
        byteLength,
        offset: this.offset,
        read(backend, baseOffset) {
          const methodName = `get${type}` as const;

          const value = backend[methodName](
            baseOffset + field.offset,
            littleEndian
          ) as ValueType<Type>;

          return value as T;
        },
        write(backend, baseOffset, value) {
          if (type === "BigInt64" || type === "BigUint64") {
            if (typeof value !== "bigint") {
              throw new Error(`Expected bigint for ${type}, got ${typeof value}`);
            }
          } else {
            if (typeof value !== "number") {
              throw new Error(`Expected number for ${type}, got ${typeof value}`);
            }
          }

          if (allowedValues && !allowedValues.includes(value)) {
            throw new Error(`Value "${value}" is not allowed`);
          }

          const methodName = `set${type}` as const;
          backend[methodName](baseOffset + field.offset, value as never, littleEndian);
        },
      };

      return this.registerField(fieldName, field);
    };
  }

  get length(): number {
    return this.offset;
  }

  readonly addFloat16 = this.createField("Float16");
  readonly addFloat32 = this.createField("Float32");
  readonly addFloat64 = this.createField("Float64");
  readonly addInt8 = this.createField("Int8");
  readonly addInt16 = this.createField("Int16");
  readonly addInt32 = this.createField("Int32");
  readonly addUint8 = this.createField("Uint8");
  readonly addUint16 = this.createField("Uint16");
  readonly addUint32 = this.createField("Uint32");
  readonly addBigInt64 = this.createField("BigInt64");
  readonly addBigUint64 = this.createField("BigUint64");

  addArrayBuffer<FieldName extends string>(fieldName: FieldName, byteLength: number) {
    if (byteLength <= 0 || !Number.isInteger(byteLength)) {
      throw new Error(`Invalid byteLength: ${byteLength}. Must be a positive integer.`);
    }

    const field: FieldDefinition<ArrayBufferLike> = {
      byteLength: byteLength,
      offset: this.offset,
      read(backend, baseOffset) {
        const byteOffset = backend.byteOffset + baseOffset + field.offset;
        return backend.buffer.slice(byteOffset, byteOffset + field.byteLength);
      },
      write(backend, baseOffset, value) {
        if (value.byteLength !== field.byteLength) {
          throw new Error(
            `DataView length mismatch: expected ${field.byteLength} bytes, got ${value.byteLength}`
          );
        }

        const src = new Uint8Array(value);
        const destOffset = backend.byteOffset + baseOffset + field.offset;
        const dest = new Uint8Array(backend.buffer, destOffset, field.byteLength);
        dest.set(src);
      },
    };

    return this.registerField(fieldName, field);
  }

  addDataView<FieldName extends string>(fieldName: FieldName, byteLength: number) {
    if (byteLength <= 0 || !Number.isInteger(byteLength)) {
      throw new Error(`Invalid byteLength: ${byteLength}. Must be a positive integer.`);
    }

    const field: FieldDefinition<DataView> = {
      byteLength: byteLength,
      offset: this.offset,
      read(backend, baseOffset) {
        const byteOffset = backend.byteOffset + baseOffset + field.offset;
        return new DataView(backend.buffer, byteOffset, field.byteLength);
      },
      write(backend, baseOffset, value) {
        if (value.byteLength !== field.byteLength) {
          throw new Error(
            `DataView length mismatch: expected ${field.byteLength} bytes, got ${value.byteLength}`
          );
        }

        const src = new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
        const destOffset = backend.byteOffset + baseOffset + field.offset;
        const dest = new Uint8Array(backend.buffer, destOffset, field.byteLength);
        dest.set(src);
      },
    };

    return this.registerField(fieldName, field);
  }

  addString<FieldName extends string, T extends string = string>(
    fieldName: FieldName,
    maxByteLength: number,
    allowedValues?: readonly T[]
  ) {
    if (maxByteLength <= 0 || !Number.isInteger(maxByteLength)) {
      throw new Error(`Invalid maxByteLength: ${maxByteLength}. Must be a positive integer.`);
    }

    const prefixSize = getByteLengthForInteger(maxByteLength);
    const encoder = new TextEncoder();
    const decoder = new TextDecoder();

    const field: FieldDefinition<T> = {
      byteLength: maxByteLength + prefixSize,
      offset: this.offset,
      read(backend, baseOffset) {
        let length = 0n;
        for (let i = 0; i < prefixSize; i++) {
          const byte = BigInt(backend.getUint8(baseOffset + field.offset + i));
          length |= byte << BigInt((prefixSize - 1 - i) * 8);
        }

        if (length > BigInt(maxByteLength)) {
          throw new Error(`String length prefix ${length} exceeds maximum ${maxByteLength}`);
        }

        const stringBytes = new Uint8Array(
          backend.buffer,
          backend.byteOffset + baseOffset + field.offset + prefixSize,
          Number(length)
        );

        const value = decoder.decode(stringBytes);

        return value as T;
      },
      write(backend, baseOffset, value) {
        if (allowedValues && !allowedValues.includes(value)) {
          throw new Error(`Value "${value}" is not allowed`);
        }

        const encoded = encoder.encode(value);
        if (encoded.length > maxByteLength) {
          throw new Error(
            `String too long: ${encoded.length} bytes > ${maxByteLength} byte maximum`
          );
        }

        for (let i = 0; i < prefixSize; i++) {
          const byte = (encoded.length >> ((prefixSize - 1 - i) * 8)) & 0xff;
          backend.setUint8(baseOffset + field.offset + i, byte);
        }

        const destOffset = backend.byteOffset + baseOffset + field.offset + prefixSize;
        const dest = new Uint8Array(backend.buffer, destOffset, maxByteLength);
        dest.set(encoded);
      },
    };

    return this.registerField(fieldName, field);
  }

  clone(): DataViewSchema<Fields> {
    const schema = new DataViewSchema<Fields>();
    schema.offset = this.offset;
    Object.assign(schema.fields, this.fields);
    return schema;
  }

  getField<FieldName extends keyof Fields>(
    backend: DataViewBackend,
    baseOffset: number,
    fieldName: FieldName
  ): ReturnType<Fields[FieldName]["read"]> {
    return this.fields[fieldName].read(backend, baseOffset);
  }

  setField<FieldName extends keyof Fields>(
    backend: DataViewBackend,
    baseOffset: number,
    fieldName: FieldName,
    value: ReturnType<Fields[FieldName]["read"]>
  ): void {
    this.fields[fieldName].write(backend, baseOffset, value);
  }

  createProxyObject(
    backend: DataViewBackend,
    baseOffset: number = 0,
    callbacks?: ProxyCallbacks<Fields>
  ) {
    const requiredEndOffset = backend.byteOffset + baseOffset + this.offset;
    if (requiredEndOffset > backend.buffer.byteLength) {
      throw new Error(`Buffer size ${backend.buffer.byteLength} is too small`);
    }

    const obj = {} as { [K in keyof Fields]: ReturnType<Fields[K]["read"]> };

    for (const [fieldName, field] of Object.entries(this.fields)) {
      Object.defineProperty(obj, fieldName, {
        enumerable: true,
        get: () => {
          callbacks?.beforeRead?.(fieldName);
          const value = field.read(backend, baseOffset);
          callbacks?.afterRead?.(fieldName, value);
          return value;
        },
        set: (value: ReturnType<Fields[typeof fieldName]["read"]>) => {
          callbacks?.beforeWrite?.(fieldName, value);
          field.write(backend, baseOffset, value);
          callbacks?.afterWrite?.(fieldName);
        },
      });
    }

    return obj;
  }

  createAtomicProxyObject(backend: DataViewBackend, baseOffset: number = 0, lock: Int32Array) {
    return this.createProxyObject(backend, baseOffset, {
      beforeWrite() {
        while (Atomics.compareExchange(lock, 0, 0, 1) !== 0) Atomics.wait(lock, 0, 0);
      },
      afterWrite() {
        Atomics.store(lock, 0, 0);
        Atomics.notify(lock, 0);
      },
      beforeRead() {
        while (Atomics.compareExchange(lock, 0, 0, 1) !== 0) Atomics.wait(lock, 0, 0);
      },
      afterRead() {
        Atomics.store(lock, 0, 0);
      },
    });
  }
}
