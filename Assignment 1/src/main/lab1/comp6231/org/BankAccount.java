package lab1.comp6231.org;

public abstract class BankAccount {

	protected int accNum;
	protected double balance;
	protected static int serialNum = 1;
	
	/** Default constructor 
	 * 
	 */
	public BankAccount()
	{
        // TODO
	    // check the balance
		this.balance = 0.0; 
        // check the account Number
		this.accNum = BankAccount.serialNum++;
	}
	
	/** Overloaded constructor
	 */
	public BankAccount( double startBalance) throws Exception
	{
        // TODO
        //check the account number
	    this.balance = startBalance;
	}
	
	/** accessor for balance
	 * 
	 */
	public double getBalance()
	{
        // TODO
        return this.balance;
    }
	
	/* accessor for account number
	 * 
	 */
	public int getAccNum()
	{
		return accNum;
	}
	
	/** Deposit amount to account
	 * 
	 */
	public void deposit( double amount ) throws Exception
	{
        // TODO
        // deposit amount of money, if it is legal/valid amount
		if (amount >= 0.0) 
		{
			this.balance += amount;
		}
	}
	
	/** withdraw amount from account
	 * 
	 */
	public void withdraw( double amount ) throws Exception
	{
		if(amount >= 0.0 && amount <= balance)
			balance -= amount;
		
		else
			throw new Exception("Insufficient Balance");
	}

	/**Override toString()
	 *
	 */
	public String toString()
	{
		// TODO: print the balance amount for specific account type displaying the account number.
		return "The balance is of the " + this.accType() +" "+ this.getAccNum() + " is " + this.getBalance();
	}
	
	public abstract void applyComputation();
	public abstract String accType();
}