#define _CRT_SECURE_NO_WARNINGS

#include <fstream>
#include <iostream>
#include <ctime>

#include "Interpreter.h"
#include "CatalogManager.h"
#include "RecordManager.h"	
#include "IndexManager.h"
#include "API.h"

using namespace std;

clock_t start; //calculate the time a operation costs

int main(){
    //open the whole index file
	FILE *fp;
	fp = fopen("INDEX", "r");
	if (fp == NULL) {
		fp = fopen("INDEX", "a+");
	}
	fclose(fp);
    
    API api;
    CatalogManager cm;
    RecordManager rm;
    
    //connect api and other modules
    api.setRecordManager(&rm);
    api.setCatalogManager(&cm);
    IndexManager im(&api);
    api.setIndexManager(&im);
    rm.setAPI(&api);
    int fileExec = 0;
    ifstream file;
    Interpreter interpreter;
    interpreter.setAPI(&api);
    string s;
    int status = 0;

	start = 0; //start the timing

	srand(time(NULL)); //set a random number for the connection id

	cout << "Welcome to the MiniSQL." << endl;
    cout << "Your connection id is " << rand() << "." << endl <<endl;
	cout << "You can input \"help;\" to get help." <<endl;

    while(true){
        if(fileExec){
            //execute the file
            file.open(interpreter.getFilename());
            if(!file.is_open()){
                cout<<"Fail to open "<<interpreter.getFilename()<<endl;
                fileExec = 0;
                continue;
            }
            while(getline(file,s,';')){
                interpreter.interpret(s);//execute the SQL in the file line by line
            }
            file.close();
            fileExec = 0;
        }
        else{
            cout<<"\nMiniSQL > ";
            getline(cin,s,';');
			start = clock();
            status =  interpreter.interpret(s);
            if(status==2){
                //execute file
                fileExec = 1;
            }
			else if (status == -2) {
                //help information
				cout << "The data types supported by this MiniSQL are int, float and char(n)." << endl;
				cout << "Each table supports up to 32 attributes." << endl;
				cout << "You can execute select, insert, delete, create and drop commands." << endl;
				cout << "You can input 'execfile' to implement a file." << endl;
				cout << "You can input 'quit' or 'exit' to quit the system." << endl;
				cout << "Note that each command ends with \";\"." << endl;
			}
            else if(status==-1){
                //quit the minisql
                break;
            }
        }
    }

    return 0;
}
