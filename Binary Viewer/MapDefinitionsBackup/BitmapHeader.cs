using System;

using System.Collections.Generic;

//using System.Linq;

using System.Text;

using System.ComponentModel;  //this is needed or saple class

using System.Runtime.InteropServices;



    [Description("Map of entire Bitmap file")]

    [StructLayout(LayoutKind.Sequential, Pack = 1)]



    public class BITMAP_FILE

    {

    #region fields/properties we want to display in mapped structure windows; all public; in proper order



        [Description("First structure in Bitmap file - the header")]

        [Order(1)]

        public BITMAPFILEHEDER BitmapFileHeader;

        

        [Order(2)]

        [Description("Bitmap Header - possible variable length")]

        public object BitmapHeader

        {

            get 

            {

                if (BitmapHeaderDynamic.BitmapCoreHeader.bcSize > 0)

                    return BitmapHeaderDynamic.BitmapCoreHeader;

                if (BitmapHeaderDynamic.BitmapInfoHeader.biSize>0)

                    return BitmapHeaderDynamic.BitmapInfoHeader; 

                else if (BitmapHeaderDynamic.BitmapV4Header.bV4Size>0)

                   return BitmapHeaderDynamic.BitmapV4Header;

                else if (BitmapHeaderDynamic.BitmapV5Header.bV5Size>0)

                    return BitmapHeaderDynamic.BitmapV5Header;

                else

                    throw new Exception("Bitmap info Header is not populated");



            }

        }







    #endregion



    #region Required elements in every class/structure



        //Required method: Reserved_PopulateWithData

        //it is called from hosting program. 

        //Responsibility o this method is to populate class/structure with data

        public void Reserved_PopulateWithData(Func<long, long, byte[]> getData, long streamSize, long address)

        {

            BitmapFileHeader = new BITMAPFILEHEDER();

            BitmapFileHeader.Reserved_PopulateWithData(getData, streamSize, address);



            BitmapHeaderDynamic.Reserved_PopulateWithData(getData, streamSize, address);




        }

    #endregion



    }


    public struct BitmapHeaderDynamic

    {

        

        public static BITMAPCOREHEADER BitmapCoreHeader = new BITMAPCOREHEADER();

        public static BITMAPINFOHEADER BitmapInfoHeader = new BITMAPINFOHEADER();

        public static BITMAPV4HEADER BitmapV4Header = new BITMAPV4HEADER();

        public static BITMAPV5HEADER BitmapV5Header = new BITMAPV5HEADER();

        

        

        

        public static void Reserved_PopulateWithData(Func<long, long, byte[]> getData, long streamSize, long address)

        {

            //Read 

            byte[] bSize = getData(14, 4);

            UInt32 Size = (UInt32)BitConverter.ToInt32(bSize, 0);

            byte[] data;

            switch (Size)

            {

              case 12:

                BitmapCoreHeader.bcSize = Size;

                data = getData(18, Size - 4);

                BitmapCoreHeader.bcWidth = BitConverter.ToInt16(data, 0);

                BitmapCoreHeader.bcHeight = BitConverter.ToInt16(data, 2);

                BitmapCoreHeader.bcPlanes = BitConverter.ToInt16(data, 4);

                BitmapCoreHeader.bcBitCount = BitConverter.ToInt16(data, 6);

                break;

              case 40:

                BitmapInfoHeader.biSize = Size;

                data = getData(18, Size - 4);  //Read the rest of header

                BitmapInfoHeader.biWidth = BitConverter.ToInt32(data, 0);

                BitmapInfoHeader.biHeight = BitConverter.ToInt32(data, 4);

                BitmapInfoHeader.biPlanes = BitConverter.ToUInt16(data, 8);  //has to be 1

                BitmapInfoHeader.biBitCount = BitConverter.ToUInt16(data, 10);

                BitmapInfoHeader.biCompression= (BI_COMPRESSION)BitConverter.ToUInt32(data, 12);

                BitmapInfoHeader.biSizeImage = BitConverter.ToUInt32(data, 16);

                BitmapInfoHeader.biXPelsPerMeter = BitConverter.ToInt32(data, 20);

                BitmapInfoHeader.biYPelsPerMeter = BitConverter.ToInt32(data, 24);

                BitmapInfoHeader.biClrUsed = BitConverter.ToUInt32(data, 28);

                BitmapInfoHeader.biClrImportant = BitConverter.ToUInt32(data, 32);

                

                break;

              case 108:

                BitmapV4Header.bV4Size = Size;

                data = getData(18, Size - 4);  //Read the rest of header

                BitmapV4Header.bV4Width = BitConverter.ToInt32(data, 0);

                BitmapV4Header.bV4Height = BitConverter.ToInt32(data, 4);

                BitmapV4Header.bV4Planes = BitConverter.ToUInt16(data, 8);  //has to be 1

                BitmapV4Header.bV4BitCount = BitConverter.ToUInt16(data, 10);

                BitmapV4Header.bV4V4Compression = (BI_COMPRESSION)BitConverter.ToUInt32(data, 12);

                BitmapV4Header.bV4SizeImage = BitConverter.ToUInt32(data, 16);

                BitmapV4Header.bV4XPelsPerMeter = BitConverter.ToInt32(data, 20);

                BitmapV4Header.bV4YPelsPerMeter = BitConverter.ToInt32(data, 24);

                BitmapV4Header.bV4ClrUsed = BitConverter.ToUInt32(data, 28);

                BitmapV4Header.bV4ClrImportant = BitConverter.ToUInt32(data, 32);

                //Specyfic to V4 Header

                BitmapV4Header.bV4RedMask = BitConverter.ToUInt32(data, 36);

                BitmapV4Header.bV4GreenMask = BitConverter.ToUInt32(data, 40);

                BitmapV4Header.bV4BlueMask = BitConverter.ToUInt32(data, 44);

                BitmapV4Header.bV4AlphaMask = BitConverter.ToUInt32(data, 48);

                BitmapV4Header.BV4CsType = BitConverter.ToUInt32(data, 52);

                BitmapV4Header.bV4Endpoints = new CIEXYZTRIPLE();  //3*3*4 = 36 length



                BitmapV4Header.bV4Endpoints.ciexyzRed = new CIEXYZ();

                BitmapV4Header.bV4Endpoints.ciexyzGreen = new CIEXYZ();

                BitmapV4Header.bV4Endpoints.ciexyBlue = new CIEXYZ();

                        

                BitmapV4Header.bV4Endpoints.PopulateWithData(data, 56);

                BitmapV4Header.bV4GammaRed = BitConverter.ToUInt32(data, 92);

                BitmapV4Header.bV4GammaGreen = BitConverter.ToUInt32(data, 96);

                BitmapV4Header.bV4GammaBlue =  BitConverter.ToUInt32(data, 100);

                break;

              case 124:

                BitmapV5Header.bV5Size = Size;

                data = getData(18, Size - 4);  //Read the rest of header

                BitmapV5Header.bV5Width = BitConverter.ToInt32(data, 0);

                BitmapV5Header.bV5Height = BitConverter.ToInt32(data, 4);

                BitmapV5Header.bV5Planes = BitConverter.ToUInt16(data, 8);  //has to be 1

                BitmapV5Header.bV5BitCount = BitConverter.ToUInt16(data, 10);

                BitmapV5Header.bV5Compression = (BI_COMPRESSION)BitConverter.ToUInt32(data, 12);

                BitmapV5Header.bV5SizeImage = BitConverter.ToUInt32(data, 16);

                BitmapV5Header.bV5XPelsPerMeter = BitConverter.ToInt32(data, 20);

                BitmapV5Header.bV5YPelsPerMeter = BitConverter.ToInt32(data, 24);

                BitmapV5Header.bV5ClrUsed = BitConverter.ToUInt32(data, 28);

                BitmapV5Header.bV5ClrImportant = BitConverter.ToUInt32(data, 32);

                //Specyfic to V4 Header

                BitmapV5Header.bV5RedMask = BitConverter.ToUInt32(data, 36);

                BitmapV5Header.bV5GreenMask = BitConverter.ToUInt32(data, 40);

                BitmapV5Header.bV5BlueMask = BitConverter.ToUInt32(data, 44);

                BitmapV5Header.bV5AlphaMask = BitConverter.ToUInt32(data, 48);

                BitmapV5Header.bV5CSType = BitConverter.ToUInt32(data, 52);

                BitmapV5Header.bV5Endpoints = new CIEXYZTRIPLE();  //3*3*4 = 36 length


                BitmapV5Header.bV5Endpoints.ciexyzRed = new CIEXYZ();

                BitmapV5Header.bV5Endpoints.ciexyzGreen = new CIEXYZ();

                BitmapV5Header.bV5Endpoints.ciexyBlue = new CIEXYZ();

                        

               

                BitmapV5Header.bV5Endpoints.PopulateWithData(data, 56);

                BitmapV5Header.bV5GammaRed = BitConverter.ToUInt32(data, 92);

                BitmapV5Header.bV5GammaGreen = BitConverter.ToUInt32(data, 96);

                BitmapV5Header.bV5GammaBlue = BitConverter.ToUInt32(data, 100);

                //Specific to V5

                BitmapV5Header.bV5Intent = BitConverter.ToUInt32(data, 104);

                BitmapV5Header.bV5ProfileData = BitConverter.ToUInt32(data, 108);

                BitmapV5Header.bV5ProfileSize = BitConverter.ToUInt32(data, 112);

                BitmapV5Header.bV5Reserved = BitConverter.ToUInt32(data, 116);

                break;

              default:

                throw new Exception("Invalid Bitmap Header Size. Valid values are: 12, 40, 108 or 124");

            }

            

            

        }

    }

    

    

    public enum BI_COMPRESSION:int  //int32

    {

        [Description("An uncompressed format.")]

        BI_RGB=0,

        [Description("A run-length encoded (RLE) format for bitmaps with 8 bpp. The compression format is a 2-byte format consisting of a count byte followed by a byte containing a color index.")]

        BI_RLE8=1,

        [Description("An RLE format for bitmaps with 4 bpp. The compression format is a 2-byte format consisting of a count byte followed by two word-length color indexes.")]

        BI_RLE4=2,

        [Description("Specifies that the bitmap is not compressed. The members bV4RedMask, bV4GreenMask, and bV4BlueMask specify the red, green, and blue components for each pixel. This is valid when used with 16- and 32-bpp bitmaps.")]

        BI_BITFIELDS=3,

        [Description("Specifies that the image is compressed using the JPEG file interchange format.")]

        BI_JPEG=4,

        [Description("Specifies that the image is compressed using the PNG file interchange format.")]

        BI_PNG=5,

        [Description("The image is an uncompressed CMYK format.")]

        BI_CMYK = 0x000B,

        [Description(" A CMYK format that uses RLE compression for bitmaps with 8 bits per pixel. The compression uses a 2-byte format consisting of a count byte followed by a byte containing a color index.")]

        BI_CMYKRLE8 = 0x000C,

        [Description("A CMYK format that uses RLE compression for bitmaps with 4 bits per pixel. The compression uses a 2-byte format consisting of a count byte followed by two word-length color indexes.")]

        BI_CMYKRLE4 = 0x000D

    };



    [Description("First structure in Bitmap file")]

    [StructLayout(LayoutKind.Sequential, Pack = 1)]

    public class BITMAPFILEHEDER

    {

    #region fields and/or properties we want to display in mapped structure windows; all public; in properorder



        //Fields are used to calculate proper size occupied in the file 

        //of every element of the structure. They could be private or public. 

        //If filed is public it will be displayed in mapper window



        //public property will be used to show element in mapper window



        //Another words:

        //- public properties and fields will be shown in mapper in same order as they are defined here

        // - fields, both public and private, are used in calculations o element size occupied in the file.



        [Description("Signature if Bitmap Header")]

        [Order(1)]

        public byte[] Signature = new byte[] { (byte)'B', (byte)'M' };



        [Description("Size of Bitmap File")]

        [Order(2)]

        public UInt32 FileSize;



        [Description("Reserved 4 bytes; usually 0")]

        [Order(3)]

        public byte[] Reserved;  //4 bytes



        [Description("Offset to Roaseter Data")]

        [Order(4)]

        public UInt32 FileOffsetToRasterdata;

    #endregion



    #region Required element in every class/structure

        //NAme Reserved_PopulateWithData is reserved. 

        //IT HAS TO BE PRESENT in each class/structure deinition

        //Hosting program calls this method to instantiate with date

        //One o parameters: "callback" is allows you to get data needed 

        //to populate structure/class as stream o bytes.



        public void Reserved_PopulateWithData(Func<long, long, byte[]> getData, long streamSize, long adress)

        {

            byte[] RetVal = getData(0, 14);

            if (RetVal != null)

            {

                //check for signature

                if (!CheckSignature(RetVal, 0))

                {

                    throw new Exception("Bad signature expecting 'BM' at offset 0");

                }

                Signature[0] = RetVal[0];

                Signature[1] = RetVal[1];



                //

                FileSize = BitConverter.ToUInt32(RetVal, 2);



                Reserved = new byte[] { RetVal[6], RetVal[7], RetVal[8], RetVal[9] };



                FileOffsetToRasterdata = BitConverter.ToUInt32(RetVal, 10);



            }

        }

    #endregion



    #region other methods (no propeerties allowed)

        private bool CheckSignature(byte[] DataToInspect, int Offset)

        {

            if (Signature != null)

            {



                if (Signature.Length + Offset > DataToInspect.Length) return false;

                for (int i=0; i < Signature.Length; i++)

                {

                    if (Signature[i] != DataToInspect[i + Offset])

                    {

                        return false;

                    }

                }

            }

            return true;

        }

    #endregion



    }



    [StructLayout(LayoutKind.Sequential)]

    public struct BITMAPV4HEADER 

    {

        [Description("The number of bytes required by the structure. Applications should use this member to determine which bitmap information header structure is being used.")]

        [Order(1)]

        public UInt32 bV4Size;    //4



        [Description("The width of the bitmap, in pixels. If bV4Compression is BI_JPEG or BI_PNG, bV4Width specifies the width of the JPEG or PNG image in pixels.")]

        [Order(2)]

        public Int32 bV4Width;   //8



        [Description("The height of the bitmap, in pixels. If bV4Height is positive, the bitmap is a bottom-up DIB and its origin is the lower-left corner. If bV4Height is negative, the bitmap is a top-down DIB and its origin is the upper-left corner.")]

        [Order(3)]

        public Int32 bV4Height;  //12



        [Description("Specifies the number of planes for the target device. This value must be set to 1")]

        [Order(4)]

        public UInt16 bV4Planes; //14



        [Description("Specifies the number of bits-per-pixel. The bV4BitCount member of the BITMAPV4HEADER structure determines the number of bits that define each pixel and the maximum number of colors in the bitmap")]

        [Order(5)]

        public UInt16 bV4BitCount;  //16   0,1,4,8,16,24,32



        [Description("The type of compression for a compressed bottom-up bitmap (top-down DIBs cannot be compressed). This member can be one of the following values")]

        [Order(6)]

        public BI_COMPRESSION bV4V4Compression;  //20



        [Description("The size, in bytes, of the image. This may be set to zero for BI_RGB bitmaps. If bV4Compression is BI_JPEG or BI_PNG, bV4SizeImage is the size of the JPEG or PNG image buffer.")]

        [Order(7)]

        public UInt32 bV4SizeImage;             //24



        [Description("The horizontal resolution, in pixels-per-meter, of the target device for the bitmap. An application can use this value to select a bitmap from a resource group that best matches the characteristics of the current device.")]

        [Order(8)]

        public Int32 bV4XPelsPerMeter;          //28



        [Description("Specifies the vertical resolution, in pixels-per-meter, of the target device for the bitmap")]

        [Order(9)]

        public Int32 bV4YPelsPerMeter;          //32



        [Description("The number of color indexes in the color table that are actually used by the bitmap. If this value is zero, the bitmap uses the maximum number of colors corresponding to the value of the bV4BitCount member for the compression mode specified by bV4Compression.")]

        [Order(10)]

        public UInt32 bV4ClrUsed;               //36



        [Description("The number of color indexes that are required for displaying the bitmap. If this value is zero, all colors are important.")]

        [Order(11)]

        public UInt32 bV4ClrImportant;          //40



        [Description("Color mask that specifies the red component of each pixel, valid only if bV4Compression is set to BI_BITFIELDS.")]

        [Order(12)]

        public UInt32 bV4RedMask;               //44



        [Description("Color mask that specifies the green component of each pixel, valid only if bV4Compression is set to BI_BITFIELDS.")]

        [Order(13)]

        public UInt32 bV4GreenMask;             //48



        [Description("Color mask that specifies the blue component of each pixel, valid only if bV4Compression is set to BI_BITFIELDS.")]

        [Order(14)]

        public UInt32 bV4BlueMask;              //52



        [Description("Color mask that specifies the alpha component of each pixel.")]

        [Order(15)]

        public UInt32 bV4AlphaMask;             //56



        [Description("The color space of the DIB. The following table lists the value for bV4CSType.")]

        [Order(16)]

        public UInt32 BV4CsType;                //60



        [Description("A CIEXYZTRIPLE structure that specifies the x, y, and z coordinates of the three colors that correspond to the red, green, and blue endpoints for the logical color space associated with the bitmap.")]

        [Order(17)]

        public CIEXYZTRIPLE bV4Endpoints;      //3*3*4+60=96



        [Description("Tone response curve for red. Used if bV4CSType is set to LCS_CALIBRATED_RGB. Specify in unsigned fixed 16.16 format. The upper 16 bits are the unsigned integer value. The lower 16 bits are the fractional part.")]

        [Order(18)]

        public UInt32 bV4GammaRed;              //100



        [Description("Tone response curve for gree. This member is ignored unless color values are calibrated RGB values and bV4CSType is set to LCS_CALIBRATED_RGB. Specify in unsigned fixed 16.16 format. The upper 16 bits are the unsigned integer value. The lower 16 bits are the fractional part.")]

        [Order(19)]

        public UInt32 bV4GammaGreen;            //104



        [Description("Tone response curve for blue. Used if bV4CSType is set to LCS_CALIBRATED_RGB. Specify in unsigned fixed 16.16 format. The upper 16 bits are the unsigned integer value. The lower 16 bits are the fractional part.")]

        [Order(20)]

        public UInt32 bV4GammaBlue;             //108



        public void Reserved_PopulateWithData(Func<long, long, byte[]> getData, long streamSize, long adress)

        {

            byte[] data = getData(0, 108);

            PopulateWithData(data, 0);

        }

        public void PopulateWithData(byte[] data, int StartAddress)

        {

            bV4Size =  BitConverter.ToUInt32(data, StartAddress + 0); //4

            bV4Width = BitConverter.ToInt32(data, StartAddress + 4);  //8

            bV4Height = BitConverter.ToInt32(data, StartAddress + 8); //12

            bV4Planes = BitConverter.ToUInt16(data, StartAddress + 12); //14

            bV4BitCount= BitConverter.ToUInt16(data, StartAddress + 14); //16

            bV4V4Compression =( BI_COMPRESSION) BitConverter.ToInt32(data, StartAddress + 16); //20

            bV4SizeImage=  BitConverter.ToUInt32(data, StartAddress + 20);  //24

            bV4XPelsPerMeter= BitConverter.ToInt32(data, StartAddress + 24);  //28

            bV4YPelsPerMeter= BitConverter.ToInt32(data, StartAddress + 28); //32

            bV4ClrUsed=  BitConverter.ToUInt32(data, StartAddress + 32);   //36

            bV4ClrImportant=  BitConverter.ToUInt32(data, StartAddress + 36);   //40

           bV4RedMask=  BitConverter.ToUInt32(data, StartAddress + 40);   //44

            bV4GreenMask=  BitConverter.ToUInt32(data, StartAddress + 44); //48

            bV4BlueMask=  BitConverter.ToUInt32(data, StartAddress + 48); //52

            bV4AlphaMask=  BitConverter.ToUInt32(data, StartAddress + 52);  //56

            BV4CsType=  BitConverter.ToUInt32(data, StartAddress + 56);  //60

            bV4Endpoints= new CIEXYZTRIPLE();

            bV4Endpoints.PopulateWithData(data, 60);  //96 

            bV4GammaRed=  BitConverter.ToUInt32(data, StartAddress + 96); //100

            bV4GammaGreen=  BitConverter.ToUInt32(data, StartAddress + 100); //104

            bV4GammaBlue=  BitConverter.ToUInt32(data, StartAddress + 104); //108



        }

        

    }  



    [StructLayout(LayoutKind.Sequential)]

    public struct BITMAPV5HEADER 

    {

        [Description("The number of bytes required by the structure. Applications should use this member to determine which bitmap information header structure is being used.")]

        [Order(1)]

        public UInt32 bV5Size;  //4



        [Description("Specifies the width of the bitmap, in pixels. If bV5Compression is BI_JPEG or BI_PNG, the bV5Width member specifies the width of the decompressed JPEG or PNG image in pixels.")]

        [Order(2)]

        public Int32 bV5Width;   //8



        [Description("The height of the bitmap, in pixels. If the value of bV5Height is positive, the bitmap is a bottom-up DIB and its origin is the lower-left corner. If bV5Height value is negative, the bitmap is a top-down DIB and its origin is the upper-left corner.")]

        [Order(3)]

        public Int32 bV5Height;   //12



        [Description("The number of planes for the target device. This value must be set to 1.")]

        [Order(4)]

        public UInt16 bV5Planes;   //14



        [Description("The number of bits that define each pixel and the maximum number of colors in the bitmap.")]

        [Order(5)]

        public UInt16 bV5BitCount;   //16



        [Description("Specifies that the bitmap is not compressed. The bV5RedMask, bV5GreenMask, and bV5BlueMask members specify the red, green, and blue components of each pixel. This is valid when used with 16- and 32-bpp bitmaps")]

        [Order(6)]

        public BI_COMPRESSION bV5Compression;    //20 (int)



        [Description("The size, in bytes, of the image. This may be set to zero for BI_RGB bitmaps.")]

        [Order(7)]

        public UInt32 bV5SizeImage;         //24



        [Description("The horizontal resolution, in pixels-per-meter, of the target device for the bitmap. An application can use this value to select a bitmap from a resource group that best matches the characteristics of the current device.")]

        [Order(8)]

        public Int32 bV5XPelsPerMeter;      //28



        [Description("The vertical resolution, in pixels-per-meter, of the target device for the bitmap.")]

        [Order(9)]

        public Int32 bV5YPelsPerMeter;      //32



        [Description("The number of color indexes in the color table that are actually used by the bitmap. If this value is zero, the bitmap uses the maximum number of colors corresponding to the value of the bV5BitCount member for the compression mode specified by bV5Compression.")]

        [Order(10)]

        public UInt32 bV5ClrUsed;           //36



        [Description("The number of color indexes that are required for displaying the bitmap. If this value is zero, all colors are required.")]

        [Order(11)]

        public UInt32 bV5ClrImportant;      //40



        [Description("Color mask that specifies the red component of each pixel, valid only if bV5Compression is set to BI_BITFIELDS.")]

        [Order(12)]

        public UInt32 bV5RedMask;           //44

    

        [Description("Color mask that specifies the green component of each pixel, valid only if bV5Compression is set to BI_BITFIELDS.")]

        [Order(13)]

        public UInt32 bV5GreenMask;         //48



        [Description("Color mask that specifies the blue component of each pixel, valid only if bV5Compression is set to BI_BITFIELDS.")]

        [Order(14)]

        public UInt32 bV5BlueMask;          //52



        [Description("Color mask that specifies the alpha component of each pixel.")]

        [Order(15)]

        public UInt32 bV5AlphaMask;         //56



        [Description("The color space of the DIB.")]

        [Order(16)]

        public UInt32 bV5CSType;            //60



        [Description("A CIEXYZTRIPLE structure that specifies the x, y, and z coordinates of the three colors that correspond to the red, green, and blue endpoints for the logical color space associated with the bitmap. This member is ignored unless the bV5CSType member specifies LCS_CALIBRATED_RGB.")]

        [Order(17)]

        public CIEXYZTRIPLE bV5Endpoints;       //60+ 36 =96



        [Description("Toned response curve for red. Used if bV5CSType is set to LCS_CALIBRATED_RGB. Specify in unsigned fixed 16.16 format. The upper 16 bits are the unsigned integer value. The lower 16 bits are the fractional part.")]

        [Order(18)]

        public UInt32 bV5GammaRed;              //100



        [Description("Toned response curve for green. Used if bV5CSType is set to LCS_CALIBRATED_RGB. Specify in unsigned fixed 16.16 format. The upper 16 bits are the unsigned integer value. The lower 16 bits are the fractional part.")]

        [Order(19)]

        public UInt32 bV5GammaGreen;            //104



        [Description("Toned response curve for blue. Used if bV5CSType is set to LCS_CALIBRATED_RGB. Specify in unsigned fixed 16.16 format. The upper 16 bits are the unsigned integer value. The lower 16 bits are the fractional part.")]

        [Order(20)]

        public UInt32 bV5GammaBlue;             //108



        [Description("Rendering intent for bitmap.")]

        [Order(21)]

        public UInt32 bV5Intent;                //112



        [Description("The offset, in bytes, from the beginning of the BITMAPV5HEADER structure to the start of the profile data. If the profile is embedded, profile data is the actual profile, and it is linked. (The profile data is the null-terminated file name of the profile.) This cannot be a Unicode string. It must be composed exclusively of characters from the Windows character set (code page 1252). These profile members are ignored unless the bV5CSType member specifies PROFILE_LINKED or PROFILE_EMBEDDED.")]

        [Order(22)]

        public UInt32 bV5ProfileData;           //116



        [Description("Size, in bytes, of embedded profile data.")]

        [Order(23)]

        public UInt32 bV5ProfileSize;           //120



        [Description("This member has been reserved. Its value should be set to zero.")]

        [Order(24)]

        public UInt32 bV5Reserved;              //124



        public void Reserved_PopulateWithData(Func<long, long, byte[]> getData, long streamSize, long adress)

        {

            byte[] data = getData(0, 124);

            PopulateWithData(data, 0);

        }



        public void PopulateWithData(byte[] data, int StartAddress)

        {

            bV5Size = BitConverter.ToUInt32(data, StartAddress + 0); //4

            bV5Width = BitConverter.ToInt32(data, StartAddress + 4);  //8

            bV5Height = BitConverter.ToInt32(data, StartAddress + 8); //12

            bV5Planes = BitConverter.ToUInt16(data, StartAddress + 12); //14

            bV5BitCount= BitConverter.ToUInt16(data, StartAddress + 14); //16

            bV5Compression =( BI_COMPRESSION) BitConverter.ToInt32(data, StartAddress + 16); //20

            bV5SizeImage=  BitConverter.ToUInt32(data, StartAddress + 20);  //24

            bV5XPelsPerMeter= BitConverter.ToInt32(data, StartAddress + 24);  //28

            bV5YPelsPerMeter= BitConverter.ToInt32(data, StartAddress + 28); //32

            bV5ClrUsed=  BitConverter.ToUInt32(data, StartAddress + 32);   //36

            bV5ClrImportant=  BitConverter.ToUInt32(data, StartAddress + 36);   //40

            bV5RedMask=  BitConverter.ToUInt32(data, StartAddress + 40);   //44

            bV5GreenMask=  BitConverter.ToUInt32(data, StartAddress + 44); //48

            bV5BlueMask=  BitConverter.ToUInt32(data, StartAddress + 48); //52

            bV5AlphaMask=  BitConverter.ToUInt32(data, StartAddress + 52);  //56

            bV5CSType=  BitConverter.ToUInt32(data, StartAddress + 56);  //60

            bV5Endpoints= new CIEXYZTRIPLE();



            bV5Endpoints.PopulateWithData(data, 60);  //96 

            bV5GammaRed=  BitConverter.ToUInt32(data, StartAddress + 96); //100

            bV5GammaGreen=  BitConverter.ToUInt32(data, StartAddress + 100); //104

            bV5GammaBlue=  BitConverter.ToUInt32(data, StartAddress + 104); //108



            bV5Intent=  BitConverter.ToUInt32(data, StartAddress + 108);  //112

            bV5ProfileData=  BitConverter.ToUInt32(data, StartAddress + 112);  //116

            bV5ProfileSize=  BitConverter.ToUInt32(data, StartAddress + 116);  //120

            bV5Reserved =  BitConverter.ToUInt32(data, StartAddress + 120);  //124



        }

    }



    [StructLayout(LayoutKind.Sequential)]

    [Description("The BITMAPINFOHEADER contains information about the color space and dimensions of a DIB.")]

    public class BITMAPINFOHEADER

    {

        [Description("The number of bytes required by the structure.")]

        [Order(1)]

        public UInt32 biSize;     //4     //Should be 40



        [Description("The width of the bitmap, in pixels.")]

        [Order(2)]

        public Int32 biWidth;      //8



        [Description("The height of the bitmap, in pixels. If biHeight is positive, the bitmap is a bottom-up DIB and its origin is the lower-left corner. If biHeight is negative, the bitmap is a top-down DIB and its origin is the upper-left corner.")]

        [Order(3)]

        public Int32 biHeight;     //12



        [Description("The number of planes for the target device. This value must be set to 1.")]

        [Order(4)]

        public UInt16 biPlanes;     //14



        [Description("The number of bits-per-pixel. The biBitCount member of the BITMAPINFOHEADER structure determines the number of bits that define each pixel and the maximum number of colors in the bitmap. This member must be one of the following values.")]

        [Order(5)]

        public UInt16 biBitCount;       //16



        [Description("The type of compression for a compressed bottom-up bitmap (top-down DIBs cannot be compressed).")]

        [Order(6)]

        public BI_COMPRESSION biCompression;  //Int32      //20



        [Description("The size, in bytes, of the image. This may be set to zero for BI_RGB bitmaps.")]

        [Order(7)]

        public UInt32 biSizeImage;              //24



        [Description("The horizontal resolution, in pixels-per-meter, of the target device for the bitmap. An application can use this value to select a bitmap from a resource group that best matches the characteristics of the current device.")]

        [Order(8)]

        public Int32 biXPelsPerMeter;          //28



        [Description("The vertical resolution, in pixels-per-meter, of the target device for the bitmap.")]

        [Order(9)]

        public Int32 biYPelsPerMeter;          //32



        [Order(10)]

        [Description("The number of color indexes in the color table that are actually used by the bitmap. If this value is zero, the bitmap uses the maximum number of colors corresponding to the value of the biBitCount member for the compression mode specified by biCompression.")]

        public UInt32 biClrUsed;                //36



        [Description("The number of color indexes that are required for displaying the bitmap. If this value is zero, all colors are required.")]

        [Order(11)]

        public UInt32 biClrImportant;           //40



        public void Reserved_PopulateWithData(Func<long, long, byte[]> getData, long streamSize, long adress)

        {

            byte[] data = getData(0, 40);

            PopulateWithData(data, 0);

        }

        public void PopulateWithData(byte[] data, int StartAddress)

        {

            biSize = BitConverter.ToUInt32(data, StartAddress + 0); //4

            biWidth = BitConverter.ToInt32(data, StartAddress + 4);  //8

            biHeight = BitConverter.ToInt32(data, StartAddress + 8);  //12

            biPlanes = BitConverter.ToUInt16(data, StartAddress + 12);  //14

            biBitCount = BitConverter.ToUInt16(data, StartAddress + 14);  //16

            biCompression = (BI_COMPRESSION) BitConverter.ToUInt32(data, StartAddress + 16);  //20

            biSizeImage = BitConverter.ToUInt32(data, StartAddress + 20);  //24

            biXPelsPerMeter = BitConverter.ToInt32(data, StartAddress + 24);  //28

            biYPelsPerMeter = BitConverter.ToInt32(data, StartAddress + 28);  //32

            biClrUsed = BitConverter.ToUInt32(data, StartAddress + 32); //36

            biClrImportant= BitConverter.ToUInt32(data, StartAddress + 36); //40



        }



    }



    [Description("The RGBQUAD structure describes a color consisting of relative intensities of red, green, and blue.")]

    [StructLayout(LayoutKind.Sequential, Pack=1)]

    public class RGBQUAD 

    {

        [Description("The intensity of blue in the color.")]

        [Order(1)]

        public byte rgbBlue; 



        [Description("The intensity of green in the color.")]

        [Order(2)]

        public byte rgbGreen; 



        [Description("The intensity of red in the color.")]

        [Order(3)]

        public byte rgbRed; 



        [Description("This member is reserved and must be zero.")]

        [Order(4)]

        public byte rgbReserved;



        public void Reserved_PopulateWithData(Func<long, long, byte[]> getData, long streamSize, long adress)

        {

            byte[] data = getData(0, 4);

            PopulateWithData(data, 0);

        }

        public void PopulateWithData(byte[] data, int StartAddress)

        {

            rgbBlue = data[StartAddress + 0];

            rgbGreen = data[StartAddress + 1];

            rgbRed = data[StartAddress + 2];

            rgbReserved = data[StartAddress + 3];

        }

    }



    [StructLayout(LayoutKind.Sequential, Pack = 1)]

    public class RGBTRIPLE

    {

        [Order(1)]

        public byte rgbtBlue;

        [Order(2)]

        public byte rgbtGreen;

        [Order(3)]

        public byte rgbtRed;

    

        public void Reserved_PopulateWithData(Func<long, long, byte[]> getData, long streamSize, long adress)

        {

            byte[] data = getData(0, 3);

            PopulateWithData(data, 0);

        }

        

        public void PopulateWithData(byte[] data, int StartAddress)

        {

            rgbtBlue=data[StartAddress + 0];

            rgbtGreen= data[StartAddress +1];

            rgbtRed=data[StartAddress + 2];



        }

    }



    [Description("The BITMAPCOREHEADER structure contains information about the dimensions and color format of a DIB.")]

    [StructLayout(LayoutKind.Sequential)]

    public class BITMAPCOREHEADER

    {

        [Description("The number of bytes required by the structure")]

        [Order(1)]

        public UInt32 bcSize;  //4



        [Description("The width of the bitmap, in pixels")]

        [Order(2)]

        public Int16 bcWidth;   //6



        [Description("The height of the bitmap, in pixels")]

        [Order(3)]

        public Int16 bcHeight;  //8



        [Description("The number of planes for the target device. This value must be 1.")]

        [Order(4)]

        public Int16 bcPlanes;  //10



        [Description("The number of bits-per-pixel. This value must be 1, 4, 8, or 24")]

        [Order(5)]

        public Int16 bcBitCount;    //12



        public void Reserved_PopulateWithData(Func<long, long, byte[]> getData, long streamSize, long adress)

       {

            byte[] data = getData(0, 12);

            PopulateWithData(data, 0);

        }

        public void PopulateWithData(byte[] data, int StartAddress)

        {

            bcSize = BitConverter.ToUInt32(data, StartAddress + 0); //4

            bcWidth = BitConverter.ToInt16(data, StartAddress + 4); //6

            bcHeight = BitConverter.ToInt16(data, StartAddress + 6); //8

            bcPlanes = BitConverter.ToInt16(data, StartAddress + 8); //10

            bcBitCount = BitConverter.ToInt16(data, StartAddress + 10); //12

        }

    }



    [StructLayout(LayoutKind.Sequential)]

    public class BITMAPINFO

    {

        [Order(1)]

        public BITMAPINFOHEADER bmiHeader;

        [Order(2)]

        public RGBQUAD bmiColors;



        public void Reserved_PopulateWithData(Func<long, long, byte[]> getData, long streamSize, long adress)

        {

            byte[] data = getData(0, 44);

            PopulateWithData(data, 0);

        }

        public void PopulateWithData(byte[] data, int StartAddress)

        {

            bmiHeader= new BITMAPINFOHEADER();

            bmiHeader.PopulateWithData(data, StartAddress + 0);

            bmiColors = new RGBQUAD();

            bmiColors.PopulateWithData(data, StartAddress + 40);

        }

    }



    [StructLayout(LayoutKind.Sequential)]

    public class BITMAPCOREINFO

    {

        [Order(1)]

        public BITMAPCOREHEADER bmciHeader;  //12

        [Order(2)]

        public RGBTRIPLE bmciColors;        //3  



        public void Reserved_PopulateWithData(Func<long, long, byte[]> getData, long streamSize, long adress)

        {

            byte[] data = getData(0, 15);

            PopulateWithData(data, 0);

        }

        public void PopulateWithData(byte[] data, int StartAddress)

        {

            bmciHeader = new BITMAPCOREHEADER();

            bmciHeader.PopulateWithData(data, StartAddress + 0);

            bmciColors = new RGBTRIPLE();

            bmciColors.PopulateWithData(data, StartAddress + 12);

        }



    }



    [Description("The CIEXYZ structure contains the x,y, and z coordinates of a specific color in a specified color space.")]

    [StructLayout(LayoutKind.Sequential, Pack = 1)]

    public class CIEXYZ

    {

        [Description("The x coordinate in fix point (2.30).")]

        [Order(1)]

        public Int32 ciexyzX;

        [Description("The y coordinate in fix point (2.30).")]

        [Order(2)]

        public Int32 ciexyzY;

        [Description("The z coordinate in fix point (2.30).")]

         [Order(3)]

       public Int32 ciexyzZ;



        public void Reserved_PopulateWithData(Func<long, long, byte[]> getData, long streamSize, long adress)

        {

            byte[] data = getData(0, 12);

            PopulateWithData(data, 0);

        }

        

        public  void PopulateWithData(byte[] data, int StartAddress)

        {

             ciexyzX =BitConverter.ToInt32(data, StartAddress + 0);

             ciexyzY =BitConverter.ToInt32(data, StartAddress + 4);

             ciexyzZ =BitConverter.ToInt32(data, StartAddress + 8);

        }

    }



    [Description("The CIEXYZTRIPLE structure contains the x,y, and z coordinates of the three colors that correspond to the red, green, and blue endpoints for a specified logical color space.")]

    [StructLayout(LayoutKind.Sequential, Pack = 1)]

    public class CIEXYZTRIPLE

    {

        [Description("The xyz coordinates of red endpoint.")]

        public CIEXYZ ciexyzRed;

        [Description("The xyz coordinates of green endpoint.")]

        public CIEXYZ ciexyzGreen;

        [Description("The xyz coordinates of blue endpoint.")]

        public CIEXYZ ciexyBlue;



        

        public void Reserved_PopulateWithData(Func<long, long, byte[]> getData, long streamSize, long adress)

        {

            byte[] data = getData(0, 36);

            PopulateWithData(data, 0);

        }

    

        public  void PopulateWithData(byte[] data, int startAddress)

        {

            ciexyzRed= new CIEXYZ();

            ciexyzRed.ciexyzX =   BitConverter.ToInt32(data, startAddress + 0);  

            ciexyzRed.ciexyzY =   BitConverter.ToInt32(data, startAddress +  4);  

            ciexyzRed.ciexyzZ =   BitConverter.ToInt32(data, startAddress +  8);  

 

            ciexyzGreen = new CIEXYZ();

            ciexyzGreen.ciexyzX =   BitConverter.ToInt32(data, startAddress +  12);  

            ciexyzGreen.ciexyzY =   BitConverter.ToInt32(data, startAddress +  16);  

            ciexyzGreen.ciexyzZ =   BitConverter.ToInt32(data, startAddress +  20);  



            ciexyBlue  = new CIEXYZ();

            ciexyBlue.ciexyzX =   BitConverter.ToInt32(data, startAddress +  24);  

            ciexyBlue.ciexyzY =   BitConverter.ToInt32(data, startAddress +  28);  

            ciexyBlue.ciexyzZ =   BitConverter.ToInt32(data, startAddress +  32);  

        

        }

    }



