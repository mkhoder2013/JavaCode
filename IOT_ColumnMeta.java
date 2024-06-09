package tendered;
// A class for a column in Metadata
public class IOT_ColumnMeta {
	private String name;
	private String regex;
	private String regexExtended;
	private String description;
	private String dataType;
	private String removetext;

	
	
	
	@Override
	public String toString() {
		return "IOT_ColumnMeta [name=" + name + ", regex=" + regex + ", regexExtended=" + regexExtended
				+ ", description=" + description + ", dataType=" + dataType + ", removetext=" + removetext + "]";
	}
	/**
	 * 
	 */
	public String getRegexExtended() {
		return regexExtended;
	}
	
	/**
	 * @param name
	 * @param regex
	 * @param regexExtended
	 * @param description
	 * @param dataType
	 * @param removetext
	 */
	public IOT_ColumnMeta(String name, String regex, String regexExtended, String description, String dataType,
			String removetext) {
		super();
		this.name = name;
		this.regex = regex;
		this.regexExtended = regexExtended;
		this.description = description;
		this.dataType = dataType;
		this.removetext = removetext;
	}
	public String getRemovetext() {
		return removetext;
	}
	public void setRemovetext(String removetext) {
		this.removetext = removetext;
	}
	public void setRegexExtended(String regexExtended) {
		this.regexExtended = regexExtended;
	}
	/**
	 * @param name
	 * @param regex
	 * @param regexExtended
	 * @param description
	 * @param dataType
	 */
	public IOT_ColumnMeta(String name, String regex, String regexExtended, String description, String dataType) {
		super();
		this.name = name;
		this.regex = regex;
		this.regexExtended = regexExtended;
		this.description = description;
		this.dataType = dataType;
		
	}
	public IOT_ColumnMeta() {
		super();
		// TODO Auto-generated constructor stub
	}

	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getRegex() {
		return regex;
	}
	public void setRegex(String regex) {
		this.regex = regex;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public String getDataType() {
		return dataType;
	}
	public void setDataType(String dataType) {
		this.dataType = dataType;
	}
	
	
	
}
