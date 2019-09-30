#include "AM.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "bf.h"
#define MAX_SCANS 20
#define MAX_OPENFILES 20
int AM_errno = AME_OK;

void parseKeyPointer(int fd,int blockno,void *key,int pointer);


typedef struct{
	int *stack;		//
	int n;
}myStack;
#define MAX_SCANS 20

typedef struct
{
	int fd;								/*anagnoristikos arithmos arxeiou*/
	int op;								/*operator anazitisis*/
	int nrblock;					/*#block pou brethike i zitoumeni timi */
	int pos_of_record;		/*thesi eggrafis mesa sto #nrblock block*/
	void * wanted_value;	/*zitoumeni timi*/
}ScanInfo;

typedef struct
{
	ScanInfo scan_array[MAX_SCANS];		/*pinakas me tis plirofories gia ta anoixta scans*/
	int scan_counter;									/*metritis twn scan*/
}OpenScans;
OpenScans * scans;

void InitStack(myStack *st,int a){
	st->stack = malloc(sizeof(int)*a);
	st->n = 0;
}

void push(myStack *st,int index){
	st->stack[st->n] = index;
	st->n++;
}

int pop(myStack *st){
	st->n--;
	return st->stack[st->n];
}

void freestack(myStack *st){
	free(st->stack);
	free(st);
}

int compareKeys(void *k1,void *k2,char attrT){
	/*******************************************
	sigkrinei ta k1 ,k2
	epsitrefei <0 ,k1<k2
						 =0 ,k1=k2
						 >0 ,k1>k2
	********************************************/
	if(attrT == 'c'){
		char * a = (char *) k1;
		char * b = (char *) k2;
		return strcmp(a,b);
	}else if(attrT == 'i'){
		int a = *(int *)k1;
		int b = *(int *)k2;
		return(a-b);
	}else{
		float a = *(float *)k1;
		float b = *(float *)k2;
		if(a-b<0.0) return -1;
		else if(a-b==0.0) return 0;
		else return 1;
	}
}


typedef struct OF_Info{
	int fd;
	char attrType1;
	int attrLength1;
	char attrType2;
	int attrLength2;
	int riza;
	int depth;
	int maxKeys;
	int maxRecs;
}OF_Info;

int OPENFILES_Index = 0;
OF_Info OPENFILES[20];

BF_Block *block;
BF_Block *newblock;

void AM_Init() {
	BF_Init(LRU);
	scans = malloc (sizeof(OpenScans));
	scans->scan_counter=0;
	BF_Block_Init(&block);
	BF_Block_Init(&newblock);
	return;
}
BF_ErrorCode errorcode;

int AM_CreateIndex(char *fileName,
	               char attrType1,
	               int attrLength1,
	               char attrType2,
	               int attrLength2) {
		int fd;
		char* data;
	  char AM1 = 'A';
		char AM2 = 'M';
		int riza = -1;


	  if((errorcode = BF_CreateFile(fileName)) != BF_OK){  //dhmiourgw to fakelo me onoma filename
							      AM_errno = AM_BF;
										return AM_errno;
			 }

	  if((errorcode = BF_OpenFile(fileName,&fd)) != BF_OK){ //anoigw to fakelo
						AM_errno = AM_BF;
		 				return AM_errno;
			 }

		if((errorcode = BF_AllocateBlock(fd,block)) != BF_OK){ //desmevw mnhmh gia to prwto block
			AM_errno = AM_BF;
			return AM_errno;
	   }

		data = BF_Block_GetData(block);
		memcpy(data,&AM1,sizeof(char));																									//A
		memcpy(data+sizeof(char),&AM2,sizeof(char));																		//M
		memcpy(data+2*sizeof(char),&attrType1,sizeof(char));														//attrType1
		memcpy(data+3*sizeof(char),&attrLength1,sizeof(int));														//attrLength1
		memcpy(data+3*sizeof(char)+sizeof(int),&attrType2,sizeof(char));								//attrType2
		memcpy(data+3*sizeof(char)+sizeof(int)+sizeof(char),&attrLength2,sizeof(int));	//attrLength2
		memcpy(data+3*sizeof(char)+sizeof(int)+sizeof(char)+sizeof(int),&riza,sizeof(int));   //riza =-1,den iparxei


		BF_Block_SetDirty(block);   //to kanw dirty
    BF_UnpinBlock(block);      //to kanw unpin giati egrapsa se auto kai de to xreiazomai pleon
    BF_CloseFile(fd);  //kleinw to fakelo

  return AME_OK;
}


int AM_DestroyIndex(char *fileName) {
	remove(fileName);

  return AME_OK;
}


int AM_OpenIndex (char *fileName) {
	int fd;
  char* data;
  char AM1;
	char AM2;
	OF_Info info;
  if((errorcode = BF_OpenFile(fileName,&fd)) != BF_OK){      //anoigw to arxeio
		AM_errno = AM_BF;
		return AM_errno;
  }
  if ((errorcode = BF_GetBlock(fd, 0, block)) != BF_OK) { //pairnw to prwto block
		AM_errno = AM_BF;
		return AM_errno;
  }

	if(fd>MAX_OPENFILES){
		AM_errno = AM_OPEN_FILES_LIMIT_ERROR;
		return AM_errno;
	}

  data = BF_Block_GetData(block);
  memcpy(&AM1,data,sizeof(char));
	memcpy(&AM2,data+sizeof(char),sizeof(char));
	if((AM1 != 'A') || (AM2 !='M')){
		/*Den einai AM arxeio , epistro	fh lathous*/
		AM_errno = AM_NOTAMFILE;
		return AM_errno;
	}
	else{
		/*perasma xaraktiristikwn arxeiou stin domi */
		memcpy(&info.attrType1,data+2*sizeof(char),sizeof(char));
		memcpy(&info.attrLength1,data+3*sizeof(char),sizeof(int));
		memcpy(&info.attrType2,data+3*sizeof(char)+sizeof(int),sizeof(char));
		memcpy(&info.attrLength2,data+3*sizeof(char)+sizeof(int)+sizeof(char),sizeof(int));
		memcpy(&info.riza,data+3*sizeof(char)+sizeof(int)+sizeof(char)+sizeof(int),sizeof(int));
		info.maxRecs = (BF_BLOCK_SIZE-sizeof(char)-sizeof(int)*2)/(info.attrLength1+info.attrLength2);
		info.maxKeys = (BF_BLOCK_SIZE-sizeof(char)-sizeof(int)*2)/(info.attrLength1+sizeof(int));
		info.depth = 0;
		info.fd = fd;
		OPENFILES[fd] = info;
	}
	BF_UnpinBlock(block);
  return fd;
}

int AM_CloseIndex (int fileDesc) {
	BF_GetBlock(fileDesc,0,block);
	/*ananeosi rizas sto block 0 ,sta metadata*/
	char * data= BF_Block_GetData(block);
	memcpy(data+sizeof(char)*4+sizeof(int)*2,&OPENFILES[fileDesc].riza,sizeof(int));
	BF_Block_SetDirty(block);
	BF_UnpinBlock(block);
	errorcode = BF_CloseFile(fileDesc);
	if(errorcode!=BF_OK){
		AM_errno = AM_BF;
		return AM_errno;
	}
	OPENFILES[fileDesc].fd=-1;
  return AME_OK;
}


int BlockIsFull(int fd, int blocknum){
	/************************************************
	*		epistrefei -1 an to block me #blocknum			*
	*	  den einai gemato														*
	***********************************************/
	BF_GetBlock(fd,blocknum,block);
	char *data = 	BF_Block_GetData(block);
	int nr , max;
	char c;
	memcpy(&c,data,sizeof(char));
	memcpy(&nr,data+sizeof(char),sizeof(int));
	if(c=='i'){
		max = OPENFILES[fd].maxKeys;
	}else{
		max = OPENFILES[fd].maxRecs;
	}
	if(nr<max){
		return -1;
	}else{
		return 1;
	}
	BF_UnpinBlock(block);
}

void AM_InitBlockInfo(int fd,char d,int *blocknum){
	/*d='i' h' 'd'*/
	/****************************************************************************************************
	*	i sinartisi desmeuei ena kainourio block kai tou pernaei to xaraktiristiko d , arithmo eggrafwn		*
	*	An prokeitai gia data block , pernaei kai ton arithmo tou aderfou block pou einai arxika kenouriou *
	*	index block : i|0																																									*
	*	data block : d|0|-1																																							*
	**************************************************************************************************/
	BF_ErrorCode errCode;
	char *data;
	BF_AllocateBlock(fd,block);
	BF_GetBlockCounter(fd,blocknum);
	(*blocknum)--;
	data = BF_Block_GetData(block);
	/*index block*/
	/*i , #eggrafwn*/
	memcpy(data,&d,sizeof(char));
	int nr = 0;
	memcpy(data+sizeof(char),&nr,sizeof(int));
	if(d=='d'){			//den exei adelfo-block
		nr = -1;
		memcpy(data+sizeof(char)+sizeof(int),&nr,sizeof(int));
	}
	BF_Block_SetDirty(block);
	BF_UnpinBlock(block);
}

void printValue(void *value,char attrT){
	/**************************************************************************************************
							i sinartisi ektiponei tin timi value ,analoga me ton tipo tis
	**************************************************************************************************/
	if(attrT == 'c'){
		char * a = (char *)value;
		printf("%s",a );
	}else if(attrT == 'i'){

		int a = *(int *)value;
		printf("%d",a );
	}else{
		float a = *(float *)value;
		printf("%.1f",a );
	}
}


void printBlock(int fd,int blocknum){
	/**************************************************************************************************
						i sinartisi ektiponei to ekastote block me arithmo blocknum
	**************************************************************************************************/
	char *data;
	BF_GetBlock(fd,blocknum,block);
	int a,brothers,curkeys=0,keys,arg,offset=0;
	char c;
	data =BF_Block_GetData(block);

	if(blocknum==0){			//to block pou einai apothikeumena ta metadata
		/*AM*/
		memcpy(&c,data+offset,sizeof(char));
		printf("%c",c);
		offset+=sizeof(char);
		memcpy(&c,data+offset,sizeof(char));
		printf("%c|",c);
		offset+=sizeof(char);
		/*attrType1*/
		memcpy(&c,data+offset,sizeof(char));
		printf("%c|",c );
		offset+=sizeof(char);
		/*attrLength1*/
		memcpy(&a,data+offset,sizeof(int));
		printf("%d|", a);
		offset+=sizeof(int);
		/*attrType2*/
		memcpy(&c,data+offset,sizeof(char));
		printf("%c|",c );
		offset+=sizeof(char);
		/*attrLength2*/
		memcpy(&a,data+offset,sizeof(int));
		printf("%d|", a);
		offset+=sizeof(int);
		/*riza*/
		memcpy(&a,data+offset,sizeof(int));
		printf("%d|", a);
		offset+=sizeof(int);
		printf("\n");
		return;
	}
	void *valuekey = malloc(OPENFILES[fd].attrLength1);
	void *value2key = malloc(OPENFILES[fd].attrLength2);
	memcpy(&c,data+offset,sizeof(char));
	printf("%c|",c);
	offset+=sizeof(char);
	memcpy(&keys,data+offset,sizeof(int));
	offset+=sizeof(int);
	memcpy(&a,data+offset,sizeof(int));
	offset+=sizeof(int);
	printf("%d|",keys );
	printf("%d|",a);
	if(c=='i'){
		arg = sizeof(int);
	}else{
		arg = OPENFILES[fd].attrLength2;
	}
	while(curkeys<keys ){
		memcpy(valuekey,data+offset,OPENFILES[fd].attrLength1);
		printValue(valuekey,OPENFILES[fd].attrType1);
		offset+=OPENFILES[fd].attrLength1;
		if(c=='i'){
			memcpy(&a,data+offset,sizeof(int));
			printf("|%d",a);
		}else{
			printf(",");
			memcpy(value2key,data+offset,OPENFILES[fd].attrLength2);
			printValue(value2key,OPENFILES[fd].attrType2);
		}
		offset+=arg;
		curkeys++;
		printf("|");
	}
	printf("\n");
	free(valuekey);
	return;
}
int splitIndexBlock(int fd,int noblock,void *newkey,int pointer,void*key){
	/*******************************************************************
	 *					spaei to index block me arithmo #blocknum							*
	 *					kai pernaei to kainourio zeugari( key,pointer )				*
	 *					ston void * newKey pernaei to proto kleidi 						*
	 *      epistrefei ton arithmo tou kainouriou block pou ftiaxtike *
	*******************************************************************/

		int new_noblock;
		AM_InitBlockInfo(fd,'i',&new_noblock);

		BF_GetBlock(fd,noblock,block);
		BF_GetBlock(fd,new_noblock,newblock);

		char *data = BF_Block_GetData(block);
		char *newdata = BF_Block_GetData(newblock);
		int keys,newkeys,newpointer,curkeys=1,offset=0;

		keys = OPENFILES[fd].maxKeys / 2;
		newkeys = OPENFILES[fd].maxKeys - keys ;
		//metakinisi deikti gia na grapso ton kenurio #keys
		offset+=sizeof(char);
		memcpy(data+offset,&keys,sizeof(int));
		//metakinisi deikti mexri to proto key
		offset+=sizeof(int)*2;
		memcpy(newkey,data+offset,OPENFILES[fd].attrLength1);
		//oso to current key einai mirkotero apo to key pou thelo na eisagw
		while(compareKeys(newkey,key,OPENFILES[fd].attrType1)<0){
			curkeys+=1;
			if(curkeys==OPENFILES[fd].maxKeys) break;
			offset+=sizeof(int)+OPENFILES[fd].attrLength1;
			memcpy(newkey,data+offset,OPENFILES[fd].attrLength1);
		}
		data = BF_Block_GetData(block);
		//curkeys = me tin thesi tou kenouriou key
		//an i thesi tou kainouriou kleidiou einai stin mesi ,newkey = me to kleidi pros eisagwgi
		if(curkeys==keys){
			memcpy(data+sizeof(char),&keys,sizeof(int));
			data+=sizeof(char)+sizeof(int)*2+(sizeof(int)+OPENFILES[fd].attrLength1)*keys;
			memcpy(newdata+sizeof(char),&newkeys,sizeof(int));
			newdata+=sizeof(char)+sizeof(int);
			memcpy(newdata,&pointer,sizeof(int));
			newdata+=sizeof(int);
			memmove(newdata,data,(sizeof(int)+OPENFILES[fd].attrLength1)*newkeys);
			memcpy(newkey,key,OPENFILES[fd].attrLength1);
		}else{
			//allios to newkey = me to 1o kleidi meta tin mesi
			newkeys--;
			//ananeono ton #keys = maxkeys/2 sto proto block
			memcpy(data+sizeof(char),&keys,sizeof(int));
			//newkey = meseo kleidi
			data+=sizeof(char)+sizeof(int)*2+(sizeof(int)+OPENFILES[fd].attrLength1)*keys;
			memcpy(newkey,data,OPENFILES[fd].attrLength1);
			data+=OPENFILES[fd].attrLength1;
			//ananeono ton #keys = maxKeys - keys -1
			memcpy(newdata+sizeof(char),&newkeys,sizeof(int));
			newdata+=sizeof(char)+sizeof(int);
			//kai metakino to periexomeno
			memmove(newdata,data,sizeof(int)+(sizeof(int)+OPENFILES[fd].attrLength1)*newkeys);
			BF_Block_SetDirty(block);
			BF_Block_SetDirty(newblock);
			//perasma pointer->key sto ekastote block
			if(curkeys<keys){
				parseKeyPointer(fd,noblock,key,pointer);
			}else{
				parseKeyPointer(fd,new_noblock,key,pointer);
			}
		}
		BF_Block_SetDirty(block);
		BF_Block_SetDirty(newblock);
		BF_UnpinBlock(block);
		BF_UnpinBlock(newblock);

		return new_noblock;
}

void parseKeyPointer(int fd,int blockno,void *key,int pointer){
	/************************************************************
	 *				pernaei ena zeugari (key,pointer) sti block 			*
	 *  			me #blocknum																			*
	************************************************************/

	char *data;
	char *tempdata =malloc(BF_BLOCK_SIZE);
	void *currKey=malloc(OPENFILES[fd].attrLength1);
	int noKeys,currKeys=0;
	BF_GetBlock(fd,blockno,block);
	data = BF_Block_GetData(block);
	data+= sizeof(char);
	memcpy(&noKeys,data,sizeof(int));			//perno ton arithmo twn keys
	noKeys++;															//auksisi
	memcpy(data,&noKeys,sizeof(int));

	if(noKeys==1){
		/*an einai to 1o key toy block,pernaei proto*/
		memcpy(data+sizeof(int)*2,key,OPENFILES[fd].attrLength1);
		memcpy(data+sizeof(int)*2+OPENFILES[fd].attrLength1,&pointer,sizeof(int));
	}else{
		/**/
		data+=sizeof(int)*2;
		/*pernei to proto kleidi tou block*/
		memcpy(currKey,data,OPENFILES[fd].attrLength1);
		/*oso to currentkey < tou key pros eisagogi , proxwra sto epomeno*/
		while(compareKeys(currKey,key,OPENFILES[fd].attrType1)<0 && currKeys<noKeys-1){
			currKeys++;
			data+=OPENFILES[fd].attrLength1+sizeof(int);
			memcpy(currKey,data,OPENFILES[fd].attrLength1);
		}
		/*	*/
		//if(currKeys<noKeys-1){
			memmove(tempdata,data,(OPENFILES[fd].attrLength1+sizeof(int))*(noKeys-currKeys-1));
		//}
		//pername tin kenuria eggrafi, stin thesi pou deixnei to offset
		memcpy(data,key,OPENFILES[fd].attrLength1);
		data+=OPENFILES[fd].attrLength1;
		memcpy(data,&pointer,sizeof(int));
		data += sizeof(int);
		//perasma twn ipolipwn
//		if(currKeys<noKeys-1){
		memmove(data,tempdata,(OPENFILES[fd].attrLength1+sizeof(int))*(noKeys-currKeys-1));
//		}

	}
	BF_Block_SetDirty(block);
	BF_UnpinBlock(block);
	free(currKey);
	free(tempdata);
}

void parseData(int fd,int blocknum,void *value1,void *value2){
	char *data;
	char *tempdata= malloc(BF_BLOCK_SIZE);
	int totalEntries,currEntries=0;
	void *currKey = malloc(OPENFILES[fd].attrLength1);
	BF_GetBlock(fd,blocknum,block);
	data = BF_Block_GetData(block);
	memcpy(&totalEntries,data+sizeof(char),sizeof(int));			//perno ton arithmo twn keys
	totalEntries++;
	memcpy(data+sizeof(char),&totalEntries,sizeof(int));	//auksano
	if(totalEntries==1){			//1h eggrafh ,pernaei 1h sto block
		memcpy(data+sizeof(char)+sizeof(int)*2,value1,OPENFILES[fd].attrLength1);
		memcpy(data+sizeof(char)+sizeof(int)*2+OPENFILES[fd].attrLength1,value2,OPENFILES[fd].attrLength2);

	}else{
		data += sizeof(char)+sizeof(int)*2;
		/* oso to currentkey < tou key pros eisagogi , proxwra sto epomeno*/
		memcpy(currKey,data,OPENFILES[fd].attrLength1);
		while((compareKeys(currKey,value1,OPENFILES[fd].attrType1)<0) && (totalEntries-1>currEntries)){
			currEntries++;
			data+=OPENFILES[fd].attrLength1+OPENFILES[fd].attrLength2;
			memcpy(currKey,data,OPENFILES[fd].attrLength1);
		}

		//if(currEntries<totalEntries-1){
		memmove(tempdata,data,(OPENFILES[fd].attrLength1+OPENFILES[fd].attrLength2)*(totalEntries-currEntries-1));
		//}
		//pername tin kenuria eggrafi
		memcpy(data,value1,OPENFILES[fd].attrLength1);
		data+=OPENFILES[fd].attrLength1;
		memcpy(data,value2,OPENFILES[fd].attrLength2);
		data += OPENFILES[fd].attrLength2;
		//perasma twn ipolipwn
		//if(currEntries<totalEntries-1){
		memmove(data,tempdata,(OPENFILES[fd].attrLength1+OPENFILES[fd].attrLength2)*(totalEntries-currEntries-1));
		//}
	}
	free(tempdata);
	free(currKey);
	BF_Block_SetDirty(block);
	BF_UnpinBlock(block);
	//BF_Block_Destroy(&block);

}
int pointerBlock(int numblock,int fd,void * value){
	/*************************************************************
	*			pernei to index block me #numblock kai epistrefei tin	 *
	* 		timi tou pointer gia to dosmeno value						 			 *
	*************************************************************/
	char *data;
	BF_GetBlock(fd,numblock,block);
	data = BF_Block_GetData(block);
	int a,noKeys,pb;
	void *key = malloc(OPENFILES[fd].attrLength1);
	char c;

	memcpy(&c,data,sizeof(char));
	data+=sizeof(char);
	memcpy(&noKeys,data,sizeof(int));
	data+=sizeof(int);
	/* perno ton 1o deikti	kai to 1o kleidi */
	memcpy(&a,data,sizeof(int));
	if(a==-1){
		AM_InitBlockInfo(fd,'d',&a);
		memcpy(data,&a,sizeof(int));
	}
	data+=sizeof(int);
	memcpy(key,data,OPENFILES[fd].attrLength1);
	data+=OPENFILES[fd].attrLength1;
	/*oso to key < value ,pare ton epomeno deikti kai to epomeno kleidi*/
	while(compareKeys(key,value,OPENFILES[fd].attrType1)< 0){
		memcpy(&a,data,sizeof(int));
		noKeys--;
		if(a==-1){
			AM_InitBlockInfo(fd,'d',&a);
			memcpy(data,&a,sizeof(int));
		}
		if(noKeys == 0){	//an exo elegksei ola ta kleidia ,epistrofh teleutaiou deikti
			free(key);			//value>apo ola ta key
			return a;
		}
		data+=sizeof(int);
		memcpy(key,data,OPENFILES[fd].attrLength1);
		data+=OPENFILES[fd].attrLength1;
	}
	BF_UnpinBlock(block);
	free(key);
	return a;
}

int splitBlock(int fd,int blocknum,void *newKey,void *value1 ,void *value2){
	/************************************************************************
		spaei to data block me #blocknum se 2 kai eisagei tin eggrafi(valeu1,value2)
		se ena apo ta 2->
		newKey = me tin 1i eggrafi tou kenouriou block
	************************************************************************/

	int new_blocknum , recs1 = 0,recs2=0,brother ;
	char *data;
	char *newdata;
	void *midkey = malloc(OPENFILES[fd].attrLength1);
	void *prevkey = malloc(OPENFILES[fd].attrLength1);
	int recs = OPENFILES[fd].maxRecs/2;
	AM_InitBlockInfo(fd,'d',&new_blocknum);
	BF_GetBlock(fd,new_blocknum,newblock);
	BF_GetBlock(fd,blocknum,block);
	newdata = BF_Block_GetData(newblock);
	data = BF_Block_GetData(block);

	memcpy(midkey,data+sizeof(char)+sizeof(int)*2+(OPENFILES[fd].attrLength1+OPENFILES[fd].attrLength2)*recs,OPENFILES[fd].attrLength1);
	//recs1==idia kleidia aristera
	memcpy(prevkey,data+sizeof(char)+sizeof(int)*2+(OPENFILES[fd].attrLength1+OPENFILES[fd].attrLength2)*(recs-1),OPENFILES[fd].attrLength1);
	while(compareKeys(midkey,prevkey,OPENFILES[fd].attrType1)==0){
		recs1++;
		if(recs1==recs) break;
		memcpy(prevkey,data+sizeof(char)+sizeof(int)*2+(OPENFILES[fd].attrLength1+OPENFILES[fd].attrLength2)*(recs-recs1-1),OPENFILES[fd].attrLength1);
	}
	//recs2==idia kleidia deksia
	memcpy(prevkey,data+sizeof(char)+sizeof(int)*2+(OPENFILES[fd].attrLength1+OPENFILES[fd].attrLength2)*(recs+1),OPENFILES[fd].attrLength1);
	while(compareKeys(midkey,prevkey,OPENFILES[fd].attrType1)==0){
		recs2++;
		if(recs2==OPENFILES[fd].maxRecs - recs -1) break;
		memcpy(prevkey,data+sizeof(char)+sizeof(int)*2+(OPENFILES[fd].attrLength1+OPENFILES[fd].attrLength2)*(recs+recs2+1),OPENFILES[fd].attrLength1);
	}
	if(recs1==0){	//to block spaei apla sti mesi
		if(OPENFILES[fd].maxRecs%2==1 && compareKeys(midkey,value1,OPENFILES[fd].attrType1)<0 && recs2==0){
			//to kenourio kleidi tha mpei deksia , bazo alli mia eggrafi aristera (misogemata block)
			recs++;
		}//else paramenei to idio , i eggrafi tha mpei aristera,i deksia an einai idia me to meseo
		recs1=recs;
	}else{
		recs1=recs+1+recs2;
	}

	/*perasma #eggarfwn*/
	recs2 = OPENFILES[fd].maxRecs - recs1;
	memcpy(data+sizeof(char),&recs1,sizeof(int));
	memcpy(newdata+sizeof(char),&recs2,sizeof(int));
	/*perasma aderfwn block*/
	memcpy(&brother,data+sizeof(char)+sizeof(int),sizeof(int));
	memcpy(data+sizeof(char)+sizeof(int),&new_blocknum,sizeof(int));
	memcpy(newdata+sizeof(char)+sizeof(int),&brother,sizeof(int));
	data+=sizeof(char)+sizeof(int)*2+(OPENFILES[fd].attrLength1+OPENFILES[fd].attrLength2)*recs1;
	/*kathorismos kleidiou gia to panw epipedo*/
	memcpy(newKey,data,OPENFILES[fd].attrLength1);
	if(recs!= OPENFILES[fd].maxRecs/2 ){ //i kenouria eggrafi tha mpei sto deksi block
		if(compareKeys(value1,newKey,OPENFILES[fd].attrType1)<0){	//kai einai i mikroteri
				memcpy(newKey,value1,OPENFILES[fd].attrLength1);			//einai to kleidi pou tha anebei sto panw epipedo
		}
	}

	memmove(newdata+sizeof(char)+sizeof(int)*2,data,(OPENFILES[fd].attrLength1+OPENFILES[fd].attrLength2)*recs2);

	BF_Block_SetDirty(block);
	BF_Block_SetDirty(newblock);
	/*perasma eggrafis*/
	if(compareKeys(value1,newKey,OPENFILES[fd].attrType1)<0){
		parseData(fd,blocknum,value1,value2);
	}else{
		parseData(fd,new_blocknum,value1,value2);
	}
	BF_UnpinBlock(block);
	BF_UnpinBlock(newblock);
	free(midkey);
	free(prevkey);
	return new_blocknum;
}

int AM_InsertEntry(int fileDesc, void *value1, void *value2) {

	int blocknum,noEntries,Recs,newRecs,nr,pb,newPointer,noKeys,Dblocknum;
	BF_Block *mainblock;
	BF_Block_Init(&mainblock);
	void *key = malloc(OPENFILES[fileDesc].attrLength1);
	void *newKey = malloc(OPENFILES[fileDesc].attrLength1);
	char *data;
	char *newData;
	char c;
	myStack *dads = malloc(sizeof(myStack));
	if(OPENFILES[fileDesc].riza==-1){						/*an einai i proti eggrafi */
		AM_InitBlockInfo(fileDesc,'i',&blocknum);	/*arxikopoieitai ena block gia tin riza*/
		OPENFILES[fileDesc].riza = blocknum;
		OPENFILES[fileDesc].depth++;							/*to bathos tou dentrou einai 1*/
		nr=-1;																					/*kenos pointer */
		BF_GetBlock(fileDesc,blocknum,mainblock);
		data = BF_Block_GetData(mainblock);
		memcpy(data+sizeof(char)+sizeof(int),&nr,sizeof(int));
		BF_Block_SetDirty(mainblock);
		BF_UnpinBlock(mainblock);

		AM_InitBlockInfo(fileDesc,'d',&Dblocknum);			/*arxikopoieitai ena data block */

		parseKeyPointer(fileDesc,blocknum,value1,Dblocknum);	/*pername to (key=value1,pointer)*/
		parseData(fileDesc,Dblocknum,value1,value2);					/*pername tin eggrafi*/

	}else{
		InitStack(dads,OPENFILES[fileDesc].depth);				/*arxikopoihsh stoivas*/
		BF_GetBlock(fileDesc,OPENFILES[fileDesc].riza,mainblock);

		pb = OPENFILES[fileDesc].riza;
		data = BF_Block_GetData(mainblock);
		memcpy(&c,data,sizeof(char));
		BF_UnpinBlock(mainblock);
		/*ksekinontas apo to block riza , katebenoume to dentro*/
		/*oso to block einai index-block sinexise na katebeneis*/
		while(c=='i'){
			push(dads,pb);															//push ton arithmo block pou exo perasei
			pb = pointerBlock(pb,fileDesc,value1);			//pb = #block sto pio kato epipedo
			BF_GetBlock(fileDesc,pb,mainblock);
			data = BF_Block_GetData(mainblock);
			memcpy(&c,data,sizeof(char));								//perno to anagnoristiko char stin arxi tou block
			BF_UnpinBlock(mainblock);
		}
		//exo ftasei sto fillo block-data
		//edo na balo to if pb==-1 kane initblockinfo
		if(BlockIsFull(fileDesc,pb)<0){									//an to block den einai gemato
			parseData(fileDesc,pb,value1,value2);					//perasma eggrafis
		}else{																					//an to block einai gemato ->spasimo block
			newPointer = splitBlock(fileDesc,pb,newKey,value1,value2);
			/*perasma (newKey,newPointer) se index block*/
			/* oso i stoiva den einai adeia */
			while(dads->n>0){
				pb = pop(dads);																			/*pare ton #block tou parapanw epipedou*/
				memcpy(key,newKey,OPENFILES[fileDesc].attrLength1);
				if(BlockIsFull(fileDesc,pb)>0){
						/*an einai gemato -> spasimo block*/
						newPointer = splitIndexBlock(fileDesc,pb,newKey,newPointer,key);
				}else{
					/* allios perna to (key,Pointer)*/
					parseKeyPointer(fileDesc,pb,key,newPointer);
					dads->n = -1;		//o deiktis mpike de xriazete na aneboume allo epipedo
				}
			}
			/*an o deiktis den exei eisaxthei se kapoio index block,simenei oti exei spasei i riza*/
			if(dads->n!=-1){
				OPENFILES[fileDesc].depth++;									/*auksanetai to bathos*/
				AM_InitBlockInfo(fileDesc,'i',&blocknum);			/*arxikopoihsh block gia tin riza*/
				BF_GetBlock(fileDesc,blocknum,block);
				data=BF_Block_GetData(block);
				data+=sizeof(char)+sizeof(int);
				//pointer stin palia riza
				memcpy(data,&(OPENFILES[fileDesc].riza),sizeof(int));		/*perasma deikti apo ti nea riza stin palia*/
				data+=sizeof(char)+sizeof(int)*2;
				BF_UnpinBlock(block);
				//key pou anebike panw
				BF_Block_SetDirty(block);
				parseKeyPointer(fileDesc,blocknum,newKey,newPointer);		/*perasma (newkey ,newpointer) gia to neo block*/
				OPENFILES[fileDesc].riza=blocknum;
			}
			freestack(dads);
		}
	}
	free(newKey);
	return AME_OK;
}


int AM_OpenIndexScan(int fileDesc, int op, void *value) {
		int num_keys=0;
		int curr_scan;
		int record_pos=-1;
		int num_blocks;
		int i;
		char found;
		char type_of_field;
		int next_block;
		int  statement;
	 	char* data;
		int pb;
		char  c;
	  void * key;
		(scans->scan_counter)++;
		//Initiliaze Block
		BF_GetBlock(fileDesc,OPENFILES[fileDesc].riza,block);
		BF_GetBlockCounter(fileDesc,&num_blocks);
		pb = OPENFILES[fileDesc].riza;
		data = BF_Block_GetData(block);
		//get number of keys
		key = BF_Block_GetData(block);
	  memcpy(&c,data,sizeof(char));
		//psaxnoume na broume se poion  kombo einai h timi pou theloume. diatrexoume tous index kombous
		while(c=='i'){
	    pb = pointerBlock(pb,fileDesc,value);
	    BF_GetBlock(fileDesc,pb,block);
	    data = BF_Block_GetData(block);
	    memcpy(&c,data,sizeof(char));
			BF_UnpinBlock(block);
	  }
			//to pb deixnei ton kombo data pou einai h timi mas.
		BF_GetBlock(fileDesc,pb,block);
		data = BF_Block_GetData(block);
		memcpy(&num_keys,data+sizeof(char),sizeof(int));
		//psaxnoume na broume tin eggrafi mas. diatrexoume olo to data block
		for (i=0;i<num_keys;i++)
		{
			memcpy(key,data+sizeof(char)+sizeof(int)+sizeof(int) + i*(OPENFILES[fileDesc].attrLength1 + OPENFILES[fileDesc].attrLength2),OPENFILES[fileDesc].attrLength1);
			if(compareKeys(key,value ,OPENFILES[fileDesc].attrType1)==0)
			{
				//brikame to kleidi mas
		     curr_scan = scans->scan_counter;
		     if (curr_scan>MAX_SCANS-1){
					 AM_errno = AM_MAXSCANS;	//ERROR: Number of scans is greater than Max Scans
					 return AM_errno;
				 }
				//kratame to file desc, to noumero tou block, kai tin thesi tis egrafis sto arxeio
				//desmeusi mnimis gia to wanted value
				 scans->scan_array[curr_scan].wanted_value = value;
		     scans->scan_array[curr_scan].fd = fileDesc;
       	 scans->scan_array[curr_scan].nrblock = pb;
     		 scans->scan_array[curr_scan].pos_of_record=i+1;
		 		 //memcpy(scans->scan_array[curr_scan].wanted_value,value,OPENFILES[fileDesc].attrLength1);
				 //printValue(scans->scan_array[curr_scan].wanted_value,OPENFILES[fileDesc].attrType1);
				 //anathetoume kai ta op codes. stis periptoseis 2, 3,5 dld not equal, less than kai less or equal than
			 	//theloume na ksekinisoume apo tin pio xamili timi (terma aristero paidi - proti egrafi) kai na diatrexoume olo to epipedo ton data block
				//opote se autes tis periptoseis kanoume to nrblock kai to pos_of_record tha einai to terma aristero block kai to 1 antistoixa.
				//stis ipoloipes periptoseis ousiastika ksekiname apo tin timi pou brikame kai pame deksia opote to nrblock kai to pos_of_record einai auta tis egrafis pou brikame
	       switch (op)
			   {
			   		case 1: //equal
			        scans->scan_array[curr_scan].op = 1;
			        break;
			      case 2: // not equal
			        scans->scan_array[curr_scan].op = 2;
							//go to the most left child. since we have a B+ tree the values we want will be from the most left to the value we have found
							BF_GetBlock(fileDesc,OPENFILES[fileDesc].riza,block);
							BF_GetBlockCounter(fileDesc,&num_blocks);
							pb = OPENFILES[fileDesc].riza;
							data = BF_Block_GetData(block);
							memcpy(&c,data,sizeof(char));
							while(c=='i'){
						    memcpy(&pb,data+sizeof(char)+sizeof(int)+sizeof(int),sizeof(int));
						    BF_GetBlock(fileDesc,pb,block);
						    data = BF_Block_GetData(block);
						    memcpy(&c,data,sizeof(char));
								BF_UnpinBlock(block);
						  }
							scans->scan_array[curr_scan].nrblock = pb;
							printf("\npb is %d\n",pb);
							scans->scan_array[curr_scan].pos_of_record = 1;
							scans->scan_array[curr_scan].pos_of_record = 1;
			        break;
			      case 3: //to pedio- kleidi less  than to value
			        scans->scan_array[curr_scan].op = 3;
							//go to the most left child. since we have a B+ tree the values we want will be from the most left to the value we have found
							BF_GetBlock(fileDesc,OPENFILES[fileDesc].riza,block);
							BF_GetBlockCounter(fileDesc,&num_blocks);
							pb = OPENFILES[fileDesc].riza;
							data = BF_Block_GetData(block);
							memcpy(&c,data,sizeof(char));
							while(c=='i'){
								memcpy(&pb,data+sizeof(char)+sizeof(int)+sizeof(int),sizeof(int));
								BF_GetBlock(fileDesc,pb,block);
								 data = BF_Block_GetData(block);
								 memcpy(&c,data,sizeof(char));
								 BF_UnpinBlock(block);
									 }
							scans->scan_array[curr_scan].nrblock = pb;
							scans->scan_array[curr_scan].pos_of_record = 1;
			        break;
			     case 4:  // greater than
							scans->scan_array[curr_scan].op = 4;
			        break;
			     case 5: //less than or equal
							printf("\nCASE LESS THAN OR EQUAL!\n");
			        scans->scan_array[curr_scan].op = 5;
							//go to the most left child. since we have a B+ tree the values we want will be from the most left to the value we have found
							if((errorcode = BF_GetBlock(fileDesc,OPENFILES[fileDesc].riza,block))!=BF_OK){
								AM_errno = AM_BF;
								return AM_errno;
							}
							pb = OPENFILES[fileDesc].riza;
							data = BF_Block_GetData(block);
							memcpy(&c,data,sizeof(char));
							while(c=='i'){
						    memcpy(&pb,data+sizeof(char)+sizeof(int)+sizeof(int),sizeof(int));
								if((errorcode = BF_GetBlock(fileDesc,pb,block))!=BF_OK){
									AM_errno = AM_BF;
									return AM_errno;
								}
						    data = BF_Block_GetData(block);
						    memcpy(&c,data,sizeof(char));
								BF_UnpinBlock(block);
						   }
							scans->scan_array[curr_scan].nrblock = pb;
							scans->scan_array[curr_scan].pos_of_record = 1;
			        break;
			     case 6: //grater than or equal
			        scans->scan_array[curr_scan].op  = 6;
			        break;
			     default:
					 		AM_errno = AM_WRONG_OP;
							return AM_errno;//ERROR: Wrong Scan OP code. OP code must be from 1 to 6
							break;
			        //ERROR;
			        }
							BF_UnpinBlock(block);
			        return curr_scan;

				}
			}
			AM_errno = AM_SCANINDEX_NOENTRY;
			return AM_errno;
}


void *AM_FindNextEntry(int scanDesc) {
//Ta data block mas exoun tin domi d|arithmos klidiwn|arithmos epomenou block (pros ta deksia)|value1|value2|value1|value2|value1|value2 ktlp
//Etsi stis entoles memcpy/memcmp pernouma ta megethi tou block: sizeof(char)|sizeof(int)|sizeof(int)|attrLength1|attrLength2|attrLength1|attrLength2|attrLength1|attrLength2
		int num_keys;
		int i,curr_scan = scans->scan_counter;
		int block_number=scans->scan_array[scanDesc].nrblock;
	  OPENFILES[scans->scan_array[scanDesc].fd].attrType2;
		char * data;
		char found=0;
		void * value = malloc(sizeof(OPENFILES[scans->scan_array[scanDesc].fd].attrLength1));
		int next_block=0;
		void * return_value = malloc(sizeof(OPENFILES[scans->scan_array[scanDesc].fd].attrLength2));
		//Initialize Block and get block data
		if((errorcode = BF_GetBlock(scans->scan_array[scanDesc].fd,scans->scan_array[scanDesc].nrblock,block))!=BF_OK){
			AM_errno = AM_BF;
			//*(int*)return_value = -5;
			return NULL;
		}

		data=BF_Block_GetData(block);
		//Edw einai to key pou tha kanoume return
		memcpy(return_value,data+sizeof(char)+sizeof(int)+sizeof(int)+OPENFILES[scans->scan_array[scanDesc].fd].attrLength1+(scans->scan_array[scanDesc].pos_of_record -1)*(OPENFILES[scans->scan_array[scanDesc].fd].attrLength1 +OPENFILES[scans->scan_array[scanDesc].fd].attrLength2),OPENFILES[scans->scan_array[scanDesc].fd].attrLength2);
		//briskoume to epomeno key

		switch (scans->scan_array[scanDesc].op){
		case 1:
			//find next equal entry
			printf("searching at block %d at pos %d\n",scans->scan_array[scanDesc].nrblock,scans->scan_array[scanDesc].pos_of_record);
			if (scans->scan_array[scanDesc].nrblock==-1)
			{
		 		AM_errno = AM_FINDNEXTENTRY_NOENTRY;
				return NULL;//Error: No other Entires can be found!
			}
			while (next_block!=-1 && found==0)
			{
				//briskoume ton arithmo kleidiwn
				memcpy(&num_keys,data+sizeof(char),sizeof(int));
				for (i=0;i<num_keys;i++)
				{
					//pare to kleidi1 apo tin parousa egrafi
					memcpy(value,data+sizeof(char)+2*sizeof(int)+ i*(OPENFILES[scans->scan_array[scanDesc].fd].attrLength1+OPENFILES[scans->scan_array[scanDesc].fd].attrLength2),OPENFILES[scans->scan_array[scanDesc].fd].attrLength1);
					//kai sigrine to me auto pou exoume brei stin anazitisi
					//gia na min briskoume sinexos tin idia egrafi prepei na checkaroume oti o arithmos tou block h h thesi tou record den einai h idia me tin egrafi pou brikame stin OpenIndexScan
					if(compareKeys(scans->scan_array[curr_scan].wanted_value,value ,OPENFILES[scans->scan_array[scanDesc].fd].attrType1)==0 && (scans->scan_array[scanDesc].nrblock!=block_number || scans->scan_array[scanDesc].pos_of_record !=i))
					{
						//brikame tin epomeni egrafi pou pliri ta kritiria mas
			 			scans->scan_array[scanDesc].nrblock = block_number;
			 			scans->scan_array[scanDesc].pos_of_record = i;
			 			found=1;
		 			}
				}
				memcpy (&next_block,data+sizeof(char)+sizeof(int), sizeof(int));
				if (next_block==-1)
				{
					AM_errno = AM_EOF;
					scans->scan_array[scanDesc].nrblock=-1;
					return NULL;
					break;
				}
				//go to next block
				BF_UnpinBlock(block);
				if((errorcode =BF_GetBlock(scans->scan_array[scanDesc].fd,next_block,block))!=BF_OK){
					AM_errno = AM_BF;
				return NULL;
				}
				data=BF_Block_GetData(block);
			}
			printf("Found next entry: ");
			return return_value;
			break;
		case 2:
			//find next not equal entry
			printf("searching at block %d at pos %d",scans->scan_array[scanDesc].nrblock,scans->scan_array[scanDesc].pos_of_record);
			if (scans->scan_array[scanDesc].nrblock==-1)
			{
				AM_errno = AM_FINDNEXTENTRY_NOENTRY;
				return NULL;
			}
			while (next_block!=-1)
			{
				//briskoume ton arithmo kleidiwn
				memcpy(&num_keys,data+sizeof(char),sizeof(int));
				printf("\n\n\nkey num is %d\n",num_keys);
				for (i=0;i<num_keys;i++)
				{
					//pare to kleidi1 apo tin parousa egrafi
					memcpy(value,data+sizeof(char)+2*sizeof(int)+ i*(OPENFILES[scans->scan_array[scanDesc].fd].attrLength1+OPENFILES[scans->scan_array[scanDesc].fd].attrLength2),OPENFILES[scans->scan_array[scanDesc].fd].attrLength1);
					//kai sigrine to me auto pou exoume brei stin anazitisi
					//gia na min briskoume sinexos tin idia egrafi prepei na checkaroume oti o arithmos tou block h h thesi tou record den einai h idia me tin egrafi pou brikame stin OpenIndexScan
					if(compareKeys(scans->scan_array[curr_scan].wanted_value,value ,OPENFILES[scans->scan_array[scanDesc].fd].attrType1)!=0 && (scans->scan_array[scanDesc].nrblock!=block_number || scans->scan_array[scanDesc].pos_of_record !=i))
					{
						//brikame tin epomeni egrafi pou pliri ta kritiria mas
			 			scans->scan_array[scanDesc].nrblock = block_number;
			 			scans->scan_array[scanDesc].pos_of_record = i;
					}
				}
				printf("------------\n");
				memcpy (&next_block,data+sizeof(char)+sizeof(int), sizeof(int));
				if (next_block==-1)
				{
					//teleiose to arxeio
					scans->scan_array[scanDesc].nrblock=-1;
					AM_errno = AM_EOF;
					return NULL;
				}
				//go to next block
				BF_UnpinBlock(block);
				BF_GetBlock(scans->scan_array[scanDesc].fd,next_block,block);
				data=BF_Block_GetData(block);
			}
			printf("next block %d\n",next_block);
			return return_value;
			break;

		case 3:
			//less than
			printf("searching at block %d at pos %d\n",scans->scan_array[scanDesc].nrblock,scans->scan_array[scanDesc].pos_of_record);
			if (scans->scan_array[scanDesc].nrblock==-1)
			{
				AM_errno = AM_FINDNEXTENTRY_NOENTRY;

				return NULL;//Error: No other Entires can be found!
			}
			while (next_block!=-1)
			{
				//briskoume ton arithmo kleidiwn
				memcpy(&num_keys,data+sizeof(char),sizeof(int));
				for (i=0;i<num_keys;i++)
				{
					//pare to kleidi1 apo tin parousa egrafi
					memcpy(value,data+sizeof(char)+2*sizeof(int)+ i*(OPENFILES[scans->scan_array[scanDesc].fd].attrLength1+OPENFILES[scans->scan_array[scanDesc].fd].attrLength2),OPENFILES[scans->scan_array[scanDesc].fd].attrLength1);
					//kai sigrine to me auto pou exoume brei stin anazitisi
					//gia na min briskoume sinexos tin idia egrafi prepei na checkaroume oti o arithmos tou block h h thesi tou record den einai h idia me tin egrafi pou brikame stin OpenIndexScan
					if(compareKeys(scans->scan_array[curr_scan].wanted_value,value ,OPENFILES[scans->scan_array[scanDesc].fd].attrType1)>0 && (scans->scan_array[scanDesc].nrblock!=block_number || scans->scan_array[scanDesc].pos_of_record !=i))
					{
						//brikame tin epomeni egrafi pou pliri ta kritiria mas
				 		scans->scan_array[scanDesc].nrblock = block_number;
				 		scans->scan_array[scanDesc].pos_of_record = i;
					}
				}
				printf("------------\n");
				memcpy (&next_block,data+sizeof(char)+sizeof(int), sizeof(int));
				if (next_block==-1)
				{
					scans->scan_array[scanDesc].nrblock;
					AM_errno = AM_EOF;
				return NULL;
				}
				//go to next block
				BF_UnpinBlock(block);
				BF_GetBlock(scans->scan_array[scanDesc].fd,next_block,block);
				data=BF_Block_GetData(block);
			}
			printf("next block %d\n",next_block);
			return return_value;
			break;

		case 4:
			//greater than
			printf("searching at block %d at pos %d",scans->scan_array[scanDesc].nrblock,scans->scan_array[scanDesc].pos_of_record);
			if (scans->scan_array[scanDesc].nrblock==-1)
			{
				AM_errno = AM_FINDNEXTENTRY_NOENTRY;
				return NULL;
			}
			while (next_block!=-1)
			{
				//briskoume ton arithmo kleidiwn
				memcpy(&num_keys,data+sizeof(char),sizeof(int));
				printf("\n\n\nkey num is %d\n",num_keys);
				for (i=0;i<num_keys;i++)
				{
					//pare to kleidi1 apo tin parousa egrafi
					memcpy(value,data+sizeof(char)+2*sizeof(int)+ i*(OPENFILES[scans->scan_array[scanDesc].fd].attrLength1+OPENFILES[scans->scan_array[scanDesc].fd].attrLength2),OPENFILES[scans->scan_array[scanDesc].fd].attrLength1);
					//kai sigrine to me auto pou exoume brei stin anazitisi
					//gia na min briskoume sinexos tin idia egrafi prepei na checkaroume oti o arithmos tou block h h thesi tou record den einai h idia me tin egrafi pou brikame stin OpenIndexScan
					if(compareKeys(scans->scan_array[curr_scan].wanted_value,value ,OPENFILES[scans->scan_array[scanDesc].fd].attrType1)<0 && (scans->scan_array[scanDesc].nrblock!=block_number || scans->scan_array[scanDesc].pos_of_record !=i))
					{
						//brikame tin epomeni egrafi pou pliri ta kritiria mas
				 		scans->scan_array[scanDesc].nrblock = block_number;
				 		scans->scan_array[scanDesc].pos_of_record = i;
					}
			}
			printf("------------\n");
			memcpy (&next_block,data+sizeof(char)+sizeof(int), sizeof(int));
			if (next_block==-1)
			{
				scans->scan_array[scanDesc].nrblock=-1;
				AM_errno = AM_EOF;
				return NULL;
				//teleiose to arxeio
			}
			//go to next block
			BF_UnpinBlock(block);
			BF_GetBlock(scans->scan_array[scanDesc].fd,next_block,block);
			data=BF_Block_GetData(block);
		}
		printf("next block %d",next_block);
		return return_value;
		break;

		case 5:
			//less than or equal
			printf("searching at block %d at pos %d",scans->scan_array[scanDesc].nrblock,scans->scan_array[scanDesc].pos_of_record);
			if (scans->scan_array[scanDesc].nrblock==-1)
			{
				AM_errno = AM_FINDNEXTENTRY_NOENTRY;
				return NULL;//Error: No other Entires can be found!
			}
			while (next_block!=-1)
			{
				//briskoume ton arithmo kleidiwn
				memcpy(&num_keys,data+sizeof(char),sizeof(int));
				printf("\n\n\nkey num is %d\n",num_keys);
				for (i=0;i<num_keys;i++)
				{
					//pare to kleidi1 apo tin parousa egrafi
					memcpy(value,data+sizeof(char)+2*sizeof(int)+ i*(OPENFILES[scans->scan_array[scanDesc].fd].attrLength1+OPENFILES[scans->scan_array[scanDesc].fd].attrLength2),OPENFILES[scans->scan_array[scanDesc].fd].attrLength1);
					//kai sigrine to me auto pou exoume brei stin anazitisi
					//gia na min briskoume sinexos tin idia egrafi prepei na checkaroume oti o arithmos tou block h h thesi tou record den einai h idia me tin egrafi pou brikame stin OpenIndexScan
					if(compareKeys(scans->scan_array[curr_scan].wanted_value,value ,OPENFILES[scans->scan_array[scanDesc].fd].attrType1)<=0 && (scans->scan_array[scanDesc].nrblock!=block_number || scans->scan_array[scanDesc].pos_of_record !=i))
					{
						//brikame tin epomeni egrafi pou pliri ta kritiria mas
				 		scans->scan_array[scanDesc].nrblock = block_number;
				 		scans->scan_array[scanDesc].pos_of_record = i;
					}
				}
				printf("------------\n");
				memcpy (&next_block,data+sizeof(char)+sizeof(int), sizeof(int));
				if (next_block==-1)
				{
					scans->scan_array[scanDesc].nrblock=-1;
					AM_errno = AM_EOF;
					return NULL;
				}
				//go to next block
				BF_UnpinBlock(block);
				BF_GetBlock(scans->scan_array[scanDesc].fd,next_block,block);
				data=BF_Block_GetData(block);
			}
			printf("next block %d",next_block);
			return return_value;
			break;

		case 6:
			printf("searching at block %d at pos %d",scans->scan_array[scanDesc].nrblock,scans->scan_array[scanDesc].pos_of_record);
			if (scans->scan_array[scanDesc].nrblock==-1)
			{
				AM_errno = AM_FINDNEXTENTRY_NOENTRY;
				return NULL;//Error: No other Entires can be found!
			}
			while (next_block!=-1)
			{
				//briskoume ton arithmo kleidiwn
				memcpy(&num_keys,data+sizeof(char),sizeof(int));
				printf("\n\n\nkey num is %d\n",num_keys);
				for (i=0;i<num_keys;i++)
				{
					//pare to kleidi1 apo tin parousa egrafi
					memcpy(value,data+sizeof(char)+2*sizeof(int)+ i*(OPENFILES[scans->scan_array[scanDesc].fd].attrLength1+OPENFILES[scans->scan_array[scanDesc].fd].attrLength2),OPENFILES[scans->scan_array[scanDesc].fd].attrLength1);
					//kai sigrine to me auto pou exoume brei stin anazitisi
					//gia na min briskoume sinexos tin idia egrafi prepei na checkaroume oti o arithmos tou block h h thesi tou record den einai h idia me tin egrafi pou brikame stin OpenIndexScan
					if(compareKeys(scans->scan_array[curr_scan].wanted_value,value ,OPENFILES[scans->scan_array[scanDesc].fd].attrType1)>=0 && (scans->scan_array[scanDesc].nrblock!=block_number || scans->scan_array[scanDesc].pos_of_record !=i))
					{
						//brikame tin epomeni egrafi pou pliri ta kritiria mas
				 		scans->scan_array[scanDesc].nrblock = block_number;
				 		scans->scan_array[scanDesc].pos_of_record = i;
					}
				}
				printf("------------\n");
				memcpy (&next_block,data+sizeof(char)+sizeof(int), sizeof(int));
				if (next_block==-1)
				{
					scans->scan_array[scanDesc].nrblock=-1;
					AM_errno = AM_EOF;
					return NULL;
				}
				//go to next block
				BF_UnpinBlock(block);
				BF_GetBlock(scans->scan_array[scanDesc].fd,next_block,block);
				data=BF_Block_GetData(block);
			}
			printf("next block %d",next_block);
			return return_value;
			break;

			default:
				AM_errno= AM_WRONG_OP;
				return NULL;//Error: Wrong Scan OP code. OP code must be from 1 to 6.
				break;
	}
}

int AM_CloseIndexScan(int scanDesc) {
	scans->scan_array[scanDesc].fd=-1;
	scans->scan_counter--;
  return AME_OK;
}


void AM_PrintError(char *errString) {

void AM_PrintError(char *errString) {
 printf("%s\n",errString);

 switch(AM_errno) {

  case -1  :
      printf("Error : End Of File was reached earlier than expected\n");
      break;
	case -2 :
	    printf("Error : %d files are already open\n",MAX_OPENFILES);
		  break;

	case -3 :
			printf("Error : Wrong Operator.Choose operator 1-6.\n");
			break;
  case -4 :
		  printf("Error : The entry you asked doesn't exist. Search failed.\n");
			break;
	case -5:
		  printf("Error : There is no next entry.\n");
      break;
	case -6 :
			printf("Error : There is no scan process\n");
			break;
	case -7 :
			printf("Error : This file is not AM file.\n");
			break;
	case -8 :
			printf("Error in BF level \n");
			BF_PrintError(errorcode);
		  break;
	case -9 :
			printf("Error : %d scans are already open\n",MAX_SCANS);
		  break;
	default	:
			printf("No error\n");
 		}
	}
}

void AM_Close() {
	BF_Block_Destroy(&block);
	BF_Block_Destroy(&newblock);
}
