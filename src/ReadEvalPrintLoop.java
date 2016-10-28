package hadoop.hbase;

import java.io.IOException;
import java.util.Scanner;

/**
 * Created by celine on 26/10/2016.
 */

public class ReadEvalPrintLoop {

    private HBaseAction actionner;

    /**
     * Conctructor of the class
     */
    public ReadEvalPrintLoop(){
        actionner = new HBaseAction();
    }

    /**
     * Function to display the menu that explains the user's choice
     */
    public void displayMenu(){
        System.out.print("Enter the character of your choice and press enter to confirm\n");
        System.out.print("\t1. Add new user with BFF (mandatory)\n");
        System.out.print("\t2. Add information to user\n");
        System.out.print("\t3. Add connection to user\n");
        System.out.print("\t4. Get one user\n");
        System.out.print("\t5. Get all rows\n");
        System.out.print("\t6. Delete a row\n");
        System.out.print("\t7. Get user's information\n");
        System.out.print("\t8. Get user's friends\n");
        System.out.print("\t9. Get user's BFF\n");
        System.out.print("\tq. Quit\n");
    }

    /**
     * Function that manages the program depending of the user's choice
     */
    public void process(){
        //Character to store the user's choice
        char entry = ' ';
        //New scanner to read the stdin
        Scanner sc = new Scanner(System.in);
        String read;
        String[] data;
        String rowKey;
        String family;
        String qualifier;
        String value;

        //The program runs in loops until the user chooses to quit
        while(entry != 'q'){
            displayMenu();
            String choice = sc.nextLine();
            //The user's choice takes only one character
            if(choice.length()>1){
                System.out.println("Please enter only one character");
            }
            else{
                entry = choice.charAt(0);
            }

            //Switch over the entry to determine what the user wanted
            switch(entry){
                case '1':
                    System.out.println("Enter the new person and his/her BFF's name as follows: fanny, ophelia");
                    read = sc.nextLine();
                    //Split the data entered by the user over the ","
                    data = read.split(",");
                    //This action takes 2 arguments, the user needs to add a BFF to its new person
                    if(data.length == 2){
                        rowKey = data[0].trim();
                        family = "friends";
                        qualifier = "BFF";
                        value = data[1].trim();
                        try{
                            if(actionner.checkRow(value)){
                                System.out.println("Adding new user and BFF");
                                actionner.addRow(rowKey, family, qualifier, value);
                            }
                            else{
                                System.out.println("The BFF name does not exist in the network");
                            }
                        }catch(IOException e){
                            e.printStackTrace();
                        }

                    }
                    else{
                        System.out.println("This action needs 2 arguments (rowKey, BFF (existing rowKey)");
                    }
                    break;
                case '2':
                    System.out.println("Enter something like rowKey, info type (optional), value");
                    read = sc.nextLine();
                    data = read.split(",");
                    if(data.length == 2){
                        rowKey = data[0].trim();
                        family = "info";
                        qualifier = "";
                        value = data[1].trim();
                        try{
                            if(actionner.checkRow(rowKey)){
                                actionner.addRow(rowKey, family, qualifier, value);
                                System.out.println("Adding information...");
                            }
                            else{
                                System.out.println("This user does not exit yet, please add him with his BFF from the network");
                            }
                        }catch(IOException e){
                            e.printStackTrace();
                        }
                    }
                    else if(data.length == 3){
                        rowKey = data[0].trim();
                        family = "info";
                        qualifier = data[1].trim();
                        value = data[2].trim();
                        try{
                            if(actionner.checkRow(rowKey)){
                                actionner.addRow(rowKey, family, qualifier, value);
                                System.out.println("Adding information...");
                            }
                            else {
                                System.out.println("This user does not exit yet, please add him with his BFF from the network");
                            }
                        }catch(IOException e){
                            e.printStackTrace();
                        }
                    }
                    else{
                        System.out.println("This action can take 2 or 3 arguments");
                    }
                    break;
                case '3':
                    System.out.println("Enter something like rowKey, name");
                    read = sc.nextLine();
                    data = read.split(",");
                    if(data.length == 2){
                        rowKey = data[0].trim();
                        family = "friends";
                        qualifier = "friends";
                        value = data[1].trim();
                        try{
                            if(actionner.checkRow(rowKey)){
                                System.out.println("Adding connection....");
                                actionner.addRow(rowKey, family, qualifier, value);
                                System.out.println(value + "added to " + rowKey + "'s friends");
                            }
                            else{
                                System.out.println("This user does not exit yet, please add him with his BFF from the network");
                            }
                        }catch(IOException e){
                            e.printStackTrace();
                        }
                    }
                    else{
                        System.out.println("This action can take 2 or 3 arguments");
                    }
                    break;
                case '4':
                    System.out.println("Enter the name of the user you want");
                    read = sc.nextLine();
                    try{
                        if(actionner.checkRow(read)){
                            System.out.println("Getting user....");
                            actionner.getRow(read);
                        }
                        else{
                            System.out.println("Sorry, this user doesn't exist. Please use the name of a valid user");
                        }
                    }catch(IOException e){
                        e.printStackTrace();

                    }
                    break;
                case '5':
                    System.out.println("Getting all users....");
                    try{
                        actionner.getAllRows();
                    }catch(IOException e){
                        e.printStackTrace();
                    }
                    break;
                case '6':
                    System.out.println("Enter the name of the user you want to delete");
                    read = sc.nextLine();
                    try{
                        if(actionner.checkRow(read)){
                            System.out.println("Deleting a row....");
                            actionner.deleteRow(read);
                            System.out.println("Row " + read + "has been successfully deleted");
                        }
                        else{
                            System.out.println("The user you are trying to delete does not exist");
                        }
                    }catch(IOException e){
                        e.printStackTrace();
                    }
                    break;
                case '7':
                    System.out.println("Enter the name of the user you want");
                    read = sc.nextLine();
                    try{
                        if(actionner.checkRow(read)){
                            System.out.println("Retrieving user information....");
                            actionner.getFamily(read, "info");
                        }
                        else{
                            System.out.println("The user you are looking for does not exist");
                        }
                    }catch(IOException e){
                        e.printStackTrace();
                    }
                    break;
                case '8':
                    System.out.println("Enter the name of the user you want");
                    read = sc.nextLine();
                    try{
                        if(actionner.checkRow(read)){
                            System.out.println("Retrieving user's friends....");
                            actionner.getFamily(read, "friends");
                        }
                        else{
                            System.out.println("The user you are looking for does not exist");
                        }
                    }catch(IOException e){
                        e.printStackTrace();
                    }
                    break;
                case '9':
                    System.out.println("Enter the name of the user you want");
                    read = sc.nextLine();
                    try{
                        if(actionner.checkRow(read)){
                            System.out.println("Retrieving user's BFF....");
                            actionner.getFamily(read, "friends", "BFF");
                        }
                        else{
                            System.out.println("The user you are looking for does not exist");
                        }
                    }catch(IOException e){
                        e.printStackTrace();
                    }
                    break;
                case 'q':
                    sc.close();
                    System.out.println("Bye, see you soon");
                    break;
                default:
                    System.out.println("Please try again wih a valid character");
                    break;
            }
        }
    }

}
