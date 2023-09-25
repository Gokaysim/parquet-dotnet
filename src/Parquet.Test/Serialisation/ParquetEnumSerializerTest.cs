using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Parquet.Serialization;
using Xunit;

namespace Parquet.Test.Serialisation {
    public class ParquetEnumSerializerTest{
        enum TestEnum1 : int {
            val1=100,
            val2=200
        }
        
        enum TestEnum2 : uint {
            val1=100,
            val2=200
        }
        
        enum TestEnum3 : short {
            val1=100,
            val2=200
        }
        enum TestEnum4 : ushort {
            val1=100,
            val2=200
        }
        
        enum TestEnum5 {
            val1,
            val2
        }

        class TestPoco {
            
            [DataMember(Order = 14)]
            
            public TestEnum5 Enum5{ get; set; }
            public TestEnum1 Enum1 { get; set; }
            public TestEnum2 Enum2 { get; set; }
            
            public TestEnum3 Enum3{ get; set; }
            public TestEnum4 Enum4{ get; set; }
        }
        [Fact]
        public async Task Int_Enum_Test() {
            var testData = new List<TestPoco>() {
                new() {
                    Enum1 = TestEnum1.val1,
                    Enum2 = TestEnum2.val1,
                    Enum3 = TestEnum3.val1,
                    Enum4 = TestEnum4.val1,
                    Enum5 = TestEnum5.val1
                },
                new() {
                    Enum1 = TestEnum1.val2,
                    Enum2 = TestEnum2.val2,
                    Enum3 = TestEnum3.val2,
                    Enum4 = TestEnum4.val2,
                    Enum5 = TestEnum5.val2
                }
            };
            var memoryStream = new MemoryStream();
            await ParquetSerializer.SerializeAsync(testData, memoryStream);
            
            var deserializedData = await ParquetSerializer.DeserializeAsync<TestPoco>(memoryStream);
            
            Assert.Equal(testData.Count,deserializedData.Count);
            Assert.Equal(testData[0].Enum1,deserializedData[0].Enum1);
            Assert.Equal(testData[1].Enum2,deserializedData[1].Enum2);
            Assert.Equal(testData[0].Enum3,deserializedData[0].Enum3);
            Assert.Equal(testData[1].Enum4,deserializedData[1].Enum4);
            
        }
    }
}