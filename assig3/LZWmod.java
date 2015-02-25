/*************************************************************************
 *  Compilation:  javac LZWmod.java
 *  Execution:    java LZW - <reset> input.txt output.lzw   (compress)
 *  Execution:    java LZW + input.lzw output.txt           (expand)
 *  Dependencies: BinaryIn.java BinaryOut.java TST.java
 *
 *  Compress or expand binary input from files using LZW.
 *
 *************************************************************************/
package assig3;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;

public class LZWmod {
  private static final int EOF = 256;           // number of input chars
  private static final int CLEAR = 257;         // flag to reset table 
  private static final int STARTING_W = 9;      // starting code bit width
  private static final int MAX_CODE_WIDTH = 16; // max code bit width
  private static final double COMPRESSION_RATIO_THRESHOLD = 1.1;    // threshold to reset table when monitoring

  /**
   * Compresses a file using adaptive LZW
   * @param inFile - file to compress
   * @param outFile - name of compressed output file (will overwrite if already exists)
   * @param reset - how to handle full table (do nothing, hard reset, monitor/reset)
   */
  public static void compress(File inFile, File outFile, TableResetPolicy reset) { 
    int W = STARTING_W;
    int L = (int) Math.pow(2, W);
    try {
      // create binary streams with files instead of command line stream redirection
      BinaryStdIn binaryIn = new BinaryStdIn(new FileInputStream(inFile));
      BinaryStdOut binaryOut = new BinaryStdOut(new PrintStream(new FileOutputStream(outFile)));
      // initialize monitoring data
      int bitsUncompressed = 0;
      int bitsCompressed = 0;
      double originalCompressionRatio = 1;
      double currentCompressionRation = 1;
      double ratioOfRatios = 1;
      boolean monitoring = false;
      
      // write 1-bit flag for reset method used in compression
      switch (reset) {
      case NONE:
        binaryOut.write(0, 1);
        break;
      case RESET:
      case MONITOR:
        binaryOut.write(1,1);
        break;
      }
      
      // read entire file to compress as a string of chars
      String input = binaryIn.readString();
      // initialize encoding table
      TST<Integer> st = new TST<Integer>();
      for (int i = 0; i < EOF; i++)
        st.put("" + (char) i, i);
      int freeCode = CLEAR+1;   // first free code is after reserved CLEAR code

      // begin compression loop
      while (input.length() > 0) {
        String longestPrefix = st.longestPrefixOf(input);  // Find max prefix of input.
        int code = st.get(longestPrefix);   // get encoding of prefix
        binaryOut.write(code, W);   // Print prefix's encoding.
        bitsCompressed += W;    // W compressed bits written
        
        int t = longestPrefix.length();        
        bitsUncompressed += t * 8; // 8 bits per char (byte) of longestPrefix
        
        // update "original" compression ratio as long as we haven't started monitoring degredation
        if (!monitoring)
          originalCompressionRatio = (double) bitsUncompressed / bitsCompressed;
        else {  // monitor ratio of final compression ratio before table filled, and current comp. ratio
          currentCompressionRation = (double) bitsUncompressed / bitsCompressed;
          ratioOfRatios = originalCompressionRatio/currentCompressionRation;
        }

        if (freeCode < L) { // still room in table
          if (t < input.length()) {
            st.put(input.substring(0, t + 1), freeCode++);  // Add prefix + next char to symbol table.
          }
        } else if (W < MAX_CODE_WIDTH) {    // still able to expand codeword/table size
          W++;
          L = (int) Math.pow(2, W);
          if (t < input.length())
            st.put(input.substring(0, t + 1), freeCode++); // Add prefix + next char to symbol table.
        } else {    // table full and code width maxed, apply reset policy
          switch (reset) {
          case MONITOR:
            monitoring = true;  // begin monitoring
            if (ratioOfRatios < COMPRESSION_RATIO_THRESHOLD) {
              break;    // do not reset
            } else {
              monitoring = false;   // reset
            }
            // FALLTHROUGH TO RESET
          case RESET:
            // reset table and reinitialize starting values
            st = new TST<Integer>();
            W = STARTING_W;
            L = (int) Math.pow(2, W);
            for (int i = 0; i < EOF; i++)
              st.put("" + (char) i, i);
            freeCode = CLEAR+1;
            binaryOut.write(CLEAR,W);   // write reset flag
            break;
          case NONE:
            break;
          }
        }
        input = input.substring(t);            // remove longestPrefix from input stream
      }
      binaryOut.write(EOF, W);
      binaryOut.close();
    } catch (FileNotFoundException ex) {
      System.err.println(ex.getMessage());
    }
  } 

  /**
   * Decompresses an LZW encoded file
   * @param inFile - file to be decompressed
   * @param outFile - name of decompressed output file (will overwrite if already exists)
   */
  public static void expand(File inFile, File outFile) {
    int W = STARTING_W;
    int L = (int) Math.pow(2, W);
    try {
      // create binary streams with files instead of command line stream redirection
      BinaryStdIn binaryIn = new BinaryStdIn(new FileInputStream(inFile));
      BinaryStdOut binaryOut = new BinaryStdOut(new PrintStream(new FileOutputStream(outFile)));
      // default policy is do not reset
      TableResetPolicy reset = TableResetPolicy.NONE;
      // read first bit flag to set reset policy
      int resetFlag = binaryIn.readInt(1);
      if (resetFlag != 0) {
        reset = TableResetPolicy.RESET;
      } 
      // create minimum String array to hold encoding table
      String[] st = new String[L];
      int freeCWIndex;  // next available codeword value

      // initialize encoding table with all 1-byte strings
      for (freeCWIndex = 0; freeCWIndex < EOF; freeCWIndex++)
        st[freeCWIndex] = "" + (char) freeCWIndex;
      
      st[freeCWIndex++] = "";   // unused, codeword for EOF
      st[freeCWIndex++] = "";   // unused, codeword for CLEAR

      // do initial read/decode/write to "catch-up" to compress algorithm
      // expandedVal is "previous pattern"
      int compressedCode = binaryIn.readInt(W);
      String expandedVal = st[compressedCode];
      binaryOut.write(expandedVal);
      
      // begin decompression loop
      while (true) {
        compressedCode = binaryIn.readInt(W);   // read 1 compressed code
        
        // check if EOF
        if (compressedCode == EOF)
          break;        
        // check if CLEAR and should reset
        if (compressedCode == CLEAR && reset != TableResetPolicy.NONE) {
          // reinitialize table and initial values
          W = STARTING_W;
          L = (int) Math.pow(2, W);
          
          st = new String[L];
          for (freeCWIndex = 0; freeCWIndex < EOF; freeCWIndex++)
            st[freeCWIndex] = "" + (char) freeCWIndex;
          
          st[freeCWIndex++] = "";   // unused, codeword for EOF
          st[freeCWIndex++] = "";   // unused, codeword for CLEAR
          
          // redo initial "catch-up" to compression algo
          // expandedVal is "previous pattern"
          compressedCode = binaryIn.readInt(W);
          expandedVal = st[compressedCode];
          binaryOut.write(expandedVal);
          continue;
        }
        
        // decode compressed code
        String newCodeword = st[compressedCode];
        if (freeCWIndex == compressedCode) 
          newCodeword = expandedVal + expandedVal.charAt(0);   // special case hack

        if (freeCWIndex < L - 1) {  // L-1 to match comp. algo output at width expansion boundary
          st[freeCWIndex++] = expandedVal + newCodeword.charAt(0);  // add new codeword to table
        } else if(W < MAX_CODE_WIDTH) { // table full, expand code width
          W++;
          L = (int) Math.pow(2, W);
          // extend and copy array for table
          // TODO test performance memory vs. speed for copy
          String[] newst = new String[L];
          for (int i=0; i<st.length; i++)
            newst[i] = st[i];
          st = newst;
          st[freeCWIndex++] = expandedVal + newCodeword.charAt(0);  // add new codeword at expanded width
        }
        
        expandedVal = newCodeword;  // previous = current
        binaryOut.write(expandedVal);   // write decompressed data
      }
      binaryIn.close();
      binaryOut.close();
    } catch (FileNotFoundException ex) {
      System.err.println(ex.getMessage());
    }
  }



  public static void main(String[] args) {
    
    // parse arguments
    if (args.length < 3) {
      System.out.println("Enter - to compress or + to decomopress");
    } else if (args[0].equals("-")) {
      if (args.length != 4) {   // compress requires reset policy and two files
        System.out.println("usage: java assig3.LZWmod - <reset_method> <input_file> <output_file>");
        System.exit(1);
      }
      TableResetPolicy resetMethod = TableResetPolicy.NONE;
      switch(args[1].toLowerCase().charAt(0)) {
      case 'n':
        resetMethod = TableResetPolicy.NONE;
        break;
      case 'r':
        resetMethod = TableResetPolicy.RESET;
        break;
      case 'm':
        resetMethod = TableResetPolicy.MONITOR;
        break;
      default:
        System.out.println("Invalid reset policy");
        System.out.println("n = none, r = reset always, m = monitor then reset");
        System.exit(1);
        break;
      }
      compress(new File(args[2]), new File(args[3]),resetMethod);
    } else if (args[0].equals("+")) {
      if (args.length != 3) {   // decompress only requires two files
        System.out.println("usage: java assig3.LZWmod + <input_file> <output_file>");
        System.exit(1);
      }
      expand(new File(args[1]), new File(args[2]));
    } else {
      System.out.println("Enter - to compress or + to decomopress");
      System.exit(1);
    }
  }
  
  protected static enum TableResetPolicy {
    NONE,
    MONITOR,
    RESET,
  }

}
