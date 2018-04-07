package assignment1;

/**
 * This is class is used for tags mainly for count and sort
 * It is also used for task3 to read the result of task3 see Task3LastMapper
 * 
 * 
 * @author taurus
 *
 */

public class TagsNumber {
	private String tagName;
	private int tagNumber;
	public TagsNumber(){
		
	}
	public TagsNumber(String tagName, int tagNumber) {
		this.tagName = tagName;
		this.tagNumber = tagNumber;
	}
	public String getTagName() {
		return tagName;
	}
	public void setTagName(String tagName) {
		this.tagName = tagName;
	}
	public int getTagNumber() {
		return tagNumber;
	}
	public void setTagNumber(int tagNumber) {
		this.tagNumber = tagNumber;
	}
	

}
