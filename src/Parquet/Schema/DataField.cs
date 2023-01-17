﻿using System;
using System.Collections;
using System.Data.SqlTypes;
using Parquet.Data;
using Parquet.File;

namespace Parquet.Schema {
    /// <summary>
    /// Field containing actual data, unlike fields containing metadata.
    /// </summary>
    public class DataField : Field {

        private bool _isNullable;
        private bool _isArray;

        /// <summary>
        /// Parquet data type of this element
        /// </summary>
        [Obsolete]
        public DataType DataType { get; }

        /// <summary>
        /// When true, this element is allowed to have nulls. Bad naming, probably should be something like IsNullable.
        /// Changes <see cref="ClrNullableIfHasNullsType"/> property accordingly.
        /// </summary>
        public bool IsNullable {
            get => _isNullable; internal set {
                _isNullable = value;
                ClrNullableIfHasNullsType = value ? ClrType.GetNullable() : ClrType;
            }
        }

        /// <summary>
        /// When true, this element is allowed to have nulls. Bad naming, probably should be something like IsNullable.
        /// </summary>
        [Obsolete("Use IsNullable instead.")]
        public bool HasNulls => IsNullable;

        /// <summary>
        /// When true, the value is an array rather than a single value.
        /// </summary>
        public bool IsArray {
            get => _isArray; internal set {
                _isArray = value;
                MaxRepetitionLevel = value ? 1 : 0;
            }
        }

        /// <summary>
        /// CLR type of this column. For nullable columns this type is not nullable.
        /// </summary>
        public Type ClrType { get; private set; }

        /// <summary>
        /// Unsupported, use at your own risk!
        /// </summary>
        public Type ClrNullableIfHasNullsType { get; set; }

        /// <summary>
        /// Creates a new instance of <see cref="DataField"/> by name and CLR type.
        /// </summary>
        /// <param name="name">Field name</param>
        /// <param name="clrType">CLR type of this field. The type is internally discovered and expanded into appropriate Parquet flags.</param>
        /// <param name="isNullable">When set, will override <see cref="IsNullable"/> attribute regardless whether passed type was nullable or not.</param>
        /// <param name="isArray">When set, will override <see cref="IsArray"/> attribute regardless whether passed type was an array or not.</param>
        /// <param name="propertyName">When set, uses this property to get the field's data.  When not set, uses the property that matches the name parameter.</param>
        public DataField(string name, Type clrType, bool? isNullable = null, bool? isArray = null, string propertyName = null)
           : base(name, SchemaType.Data) {

            Discover(clrType, out Type baseType, out bool discIsArray, out bool discIsNullable);
            ClrType = baseType;
            if(!SchemaEncoder.IsSupported(ClrType)) {
                if(baseType == typeof(DateTimeOffset)) {
                    throw new NotSupportedException($"{nameof(DateTimeOffset)} support was dropped due to numerous ambiguity issues, please use {nameof(DateTime)} from now on.");
                }
                else {
                    throw new NotSupportedException($"type {clrType} is not supported");
                }
            }

            IsNullable = isNullable.HasValue ? isNullable.Value : discIsNullable;
            IsArray = isArray.HasValue ? isArray.Value : discIsArray;
            ClrPropName = propertyName ?? name;
            MaxRepetitionLevel = IsArray ? 1 : 0;

#pragma warning disable CS0612 // Type or member is obsolete
            DataType = SchemaEncoder.FindDataType(ClrType) ?? DataType.Unspecified;
#pragma warning restore CS0612 // Type or member is obsolete
        }

        /// <summary>
        /// Creates a new instance of <see cref="DataField"/> by specifying all the required attributes.
        /// </summary>
        /// <param name="name">Field name.</param>
        /// <param name="dataType">Native Parquet type</param>
        /// <param name="isNullable">When true, the field accepts null values. Note that nullable values take slightly more disk space and computing comparing to non-nullable, but are more common.</param>
        /// <param name="isArray">When true, each value of this field can have multiple values, similar to array in C#.</param>
        /// <param name="propertyName">When set, uses this property to get the field's data.  When not set, uses the property that matches the name parameter.</param>
        [Obsolete("use constructor not referencing DataType")]
        public DataField(string name, DataType dataType, bool isNullable = true, bool isArray = false, string propertyName = null) :
            base(name, SchemaType.Data) {

            DataType = dataType;
            ClrType = SchemaEncoder.FindSystemType(dataType);
            IsNullable = isNullable;
            IsArray = isArray;
            ClrPropName = propertyName ?? name;
        }

        internal override FieldPath PathPrefix {
            set {
                Path = value + new FieldPath(Name);
            }
        }

        /// <summary>
        /// see <see cref="ThriftFooter.GetLevels(Thrift.ColumnChunk, out int, out int)"/>
        /// </summary>
        internal override void PropagateLevels(int parentRepetitionLevel, int parentDefinitionLevel) {
            MaxRepetitionLevel = parentRepetitionLevel + (IsArray ? 1 : 0);
            MaxDefinitionLevel = parentDefinitionLevel + (IsNullable ? 1 : 0);
        }

        /// <summary>
        /// Creates non-nullable uninitialised array to hold this data type.
        /// </summary>
        /// <param name="length">Exact array size</param>
        /// <returns></returns>
        internal Array CreateArray(int length) {
            return Array.CreateInstance(ClrType, length);
        }

        internal Array CreateNullableArray(int length) {
            return Array.CreateInstance(ClrNullableIfHasNullsType, length);
        }

        internal Array UnpackDefinitions(Array definedData, int[] definitionLevels, int maxDefinitionLevel) {
            Array result = CreateNullableArray(definitionLevels.Length);

            int isrc = 0;
            for(int i = 0; i < definitionLevels.Length; i++) {
                int level = definitionLevels[i];

                if(level == maxDefinitionLevel) {
                    result.SetValue(definedData.GetValue(isrc++), i);
                }
            }
            return result;
        }

        /// <inheritdoc/>
        public override string ToString() => $"{Path} ({ClrType})";

        /// <summary>
        /// Basic equality check
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj) {
            if(obj is not DataField other)
                return false;

            return base.Equals(obj) &&
                ClrType == other.ClrType &&
                IsNullable == other.IsNullable &&
                IsArray == other.IsArray;
        }

        /// <inheritdoc/>
        public override int GetHashCode() => base.GetHashCode();

        #region [ Type Resolution ]

        private static void Discover(Type t, out Type baseType, out bool isArray, out bool isNullable) {
            baseType = t;
            isArray = false;
            isNullable = false;

            //throw a useful hint
            if(t.TryExtractDictionaryType(out Type dKey, out Type dValue)) {
                throw new ArgumentException($"cannot declare a dictionary this way, please use {nameof(MapField)}.");
            }

            if(t.TryExtractEnumerableType(out Type enumItemType)) {
                baseType = enumItemType;
                isArray = true;
            }

            if(baseType.IsNullable()) {
                baseType = baseType.GetNonNullable();
                isNullable = true;
            }
        }

        #endregion
    }
}