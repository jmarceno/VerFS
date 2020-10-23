using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel; 
using System.Runtime.InteropServices;

namespace ProXoft
{
    [Description("RECT structure mapping")]
    public class RECT
    {
        [Description("Left (Int32)")]
        [Order(1)]
        public Int32 Left;
        [Description("Top (Int32)")]
        [Order(2)]
        public Int32 Top;
        [Description("Right (Int32)")]
        [Order(3)]
        public Int32 Right;
        [Description("Bottom (Int32)")]
        [Order(4)]
        public Int32 Bottom;


        public void Reserved_PopulateWithData(Func<long, long, byte[]> getData, long streamSize, long address)
        {
            byte[] data = getData(0, 16);
            Left = BitConverter.ToInt32(data, 0);
            Top = BitConverter.ToInt32(data, 4);
            Right = BitConverter.ToInt32(data, 8);
            Bottom = BitConverter.ToInt32(data, 12);

        }

    }

    [Description("POINT structure mapping")]
    public class POINT
    {
        [Description("X (Int32)")]
        [Order(1)]
        public Int32 X;
        [Description("X (Int32)")]
        [Order(2)]
        public Int32 Y;
 
        public void Reserved_PopulateWithData(Func<long, long, byte[]> getData, long streamSize, long address)
        {
            byte[] data = getData(0, 8);
            X = BitConverter.ToInt32(data, 0);
            Y = BitConverter.ToInt32(data, 4);
        }

    }


  

    [StructLayout(LayoutKind.Sequential)]
    [Description("Contains a 64-bit value representing the number of 100-nanosecond intervals since January 1, 1601 (UTC).")]
    public class FILETIME
    {
        [Description("The low-order part of the file time.")]
        [Order(1)]
        public UInt32 DateTimeLow;
        [Description("The high-order part of the file time.")]
        [Order(2)]
        public UInt32 DateTimeHigh;

        [Order(3)]
        [Size(0)]
        public DateTime Value
        {
            get
            {
                long FileTimeAsLong = ((long) UInt32.MaxValue +1) * DateTimeHigh + DateTimeLow; 
                return DateTime.FromFileTime(FileTimeAsLong);
            }
             
        }
        public void Reserved_PopulateWithData(Func<long, long, byte[]> getData, long streamSize, long address)
        {
            byte[] data = getData(0, 8);
            DateTimeLow = BitConverter.ToUInt32(data, 0);
            DateTimeHigh = BitConverter.ToUInt32(data, 4);
        }

     }

    [StructLayout(LayoutKind.Sequential)]
    public class  GUID
    {
        [Description("Specifies the guid binary data")]
        [Order(1)]
        public byte[] data;

        [Description("GUID String")]
        [Order(2)]
        [Size(0)]
        public string GuidString
        {
            get
            {
                return new Guid(data).ToString();
            }
        }

        public void Reserved_PopulateWithData(Func<long, long, byte[]>  getData, long streamSize, long address)
        {
            data = getData(0, 16);
        }
    }

    [Description("Maps 8 bytes to C# Double")]
    public class DOUBLE
    {
        
        [Description("Specifies double binary data")]
        [Order(1)]
        public byte[] data= new byte[8];
        
        [Description("Double value")]
        [Order(2)]
        [Size(0)]
        public double Value
        {
            get
            {
                return  BitConverter.ToDouble(data, 0);
            }
        }

        public void Reserved_PopulateWithData(Func<long, long, byte[]>  getData, long streamSize, long address)
        {
            data = getData(0, 8);
        }


    }

}
