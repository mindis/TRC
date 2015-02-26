import gnu.getopt.LongOpt;
import gnu.getopt.Getopt;

public class CliParser {

	public static void parse(String[] argv) {
		int c;
		String arg;
		LongOpt[] longopts = new LongOpt[1];
		// StringBuffer sb = new StringBuffer();
		longopts[0] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h');
		// longopts[1] = new LongOpt("outputdir", LongOpt.REQUIRED_ARGUMENT, sb,
		// 'o');
		// longopts[2] = new LongOpt("maximum", LongOpt.OPTIONAL_ARGUMENT, null,
		// 2);
		//
		Getopt g = new Getopt("testprog", argv, "p:h;", longopts);
		g.setOpterr(false); // We'll do our own error handling
		//
		while ((c = g.getopt()) != -1)
			switch (c) {
			case 'p':
				arg = g.getOptarg();
				ResMgr.nrPages = Integer.parseInt(arg);
				break;
			//
			case 'h':
				System.out.println("Usage: java <#pages in memory>");
				System.exit(0);
				;
			case '?':
				System.out.println("The option '" + (char) g.getOptopt()
						+ "' is not valid");
				break;
			//
			default:
				System.out.println("getopt() returned " + c);
				break;
			}
		//
		for (int i = g.getOptind(); i < argv.length; i++)
			System.out.println("Non option argv element: " + argv[i] + "\n");
	}

} // Class GetoptDemo