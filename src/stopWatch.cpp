#include "stopWatch.h"


stopWatch::stopWatch(void)
	: duration(0)
{
	start=0;
	stop=0;
}


stopWatch::~stopWatch(void)
{
}


// get current time for stopwatch purpose
void stopWatch::startTimer(void)
{
	start=clock();
}


// get end time and count time length
void stopWatch::stopTimer(void)
{
	stop=clock();
	calcDuration();
}


// calculate task's duration
void stopWatch::calcDuration(void)
{
	duration=(double)(stop-start)/CLOCKS_PER_SEC;
}


// return event's duration
double stopWatch::getDuration(void)
{
	return duration;
}
