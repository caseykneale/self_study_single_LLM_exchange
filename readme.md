# Understanding Tradeoffs in a Single LLM Interaction for Writing Code

Over the past year I have seen an extreme fixation with AI productivity, but I haven't seen compelling experiments related to the trade-offs for having used these tools. Not from professionals anyways. I've been curious about the following questions: how much time/effort am I actually saving, what quality am I getting, and what do I get out of using it intellectually. I have a good *feeling* about some aspects but I want to look at evidence for or against my own feelings. What is actually being exchanged?

To assess that I picked a project that I don't know much about, and in doing so I want to use that as a proxy for what 1 "atomic interaction" with a modern LLM trades in exchange for a prompt. Agentic programming is what many people are talking about these days, this is intentionally **not** that. Agentic programming is a composition of many of these "atomic interactions". I want to understand what is traded in 1 such transaction on a technical but also personal level.

This write up will not be extremely technical. I will not go over every line of code but I will mention wins and losses in the code, the logic, and me as an operator. Code is offered. Curious readers can inspect for themselves and see if they agree with my conclusions or not, if they so wish.

## Experiment Setup
- Aiming for ~200 lines of code outputted.
- The project won't be something I know much about, so I can be a somewhat blind participant. 
- The human participant, me, will not research the task beyond picking a new-to-me library and instructing an LLM to build an application.
- The prompt would be similar to what I would hand a colleague but more verbose to support the capabilities of these kinds of models.
- The goal is to see how far a "1 shot" with 2 different tools goes and what a human review of the results look like.

## Goals
- I want to know what I am trading in terms of quality, maintainability, and my own understanding of code that is returned.
- I want to ear-mark any failures and describe their impact on my "project". Specifically, would I go back and `reprompt`, `redesign`, or `ignore` an issue and why.
- If the tool falls short in a meaningful way I want to examine it.
- I also want to celebrate any successes. I will pretend this was a colleague handing me the start of a project for me to finish up.

## The Prompt
[The prompt](1_shot_experiment/project-prompt.md) (305 words) focused on the goal of creating a Bluetooth metadata logger using a specific crate. AI was not used to write the prompt, and I wrote it quickly. **TL;DR**: I instructed the model to use specific libraries, and the goal of the project. Also the classic "this is important please do a good job" type stuff. The goal is to create a command line application that monitors Bluetooth signals. It should log everytime a group of bluetooth signals comes into proximity of a computer and report their presence. This is just for fun, the application doesn't matter so much.

## The Models Used

People care a lot about the models used in LLM adventures. There seem to be super fans of each frontier lab. In my experience it is mostly marketting. To avoid the hype and if anyone reads this, superfans, I used 2 models I know well that have less influencers doing free advertisting. A local model, and DeepSeek's current lead.

### Qwen3.6 35b A3B
I ran this model locally. To make it interesting I used it in an off-line manner. No help from the internet and no harness. I want to experience the 1 shot result. As a side note, I intentionally picked an older blue-tooth crate so this project had any chance of success. If you are curious, the code and the models 'thoughts' are [here](1_shot_experiment/qwen36.md). Below is a list of failings and successes, I won't enumerate them all, just some high-lights.

| Category | Issue | Severity/Action |
| :--- | :--- | :--- |
| **Code** | Broken `Cargo.toml` (missing features) | Trivial |
| **Code** | General uncompilable errors | Trivial |
| **Code** | Monolithic "main" function | Reprompt/Redesign |
| **Code** | Odd logging approach | Redesign/Reprompt |
| **Logic** | Failed to design for async library requirements | **1-Shot Failure** |

**Pros:** Free/Private, admirable attempt at `HashMap` caching.

**Cost:** ~$0.02 (Electricity)

### DeepSeek V4 Pro
I'm familiar enough with DeepSeek's latest model. It's cheap, and in agentic settings it tends to perform near Anthropic offerings for programming. It should do better than the local model, if for no reason other than it has access to the internet. The [result of the interaction](1_shot_experiment/deepseekv4_pro.md) sounded well informed but had some issues.

| Category | Issue | Severity/Action |
| :--- | :--- | :--- |
| **Code** | Broken `Cargo.toml` (features/version) | Trivial |
| **Code** | Missing turbofish | Trivial |
| **Code** | Incomplete implementation (`rssi_hint`) | Trivial |
| **Code** | Poor variable naming | Reprompt |
| **Code** | Non-idiomatic Rust patterns | Reprompt |
| **Code** | Monolithic "main" function | Reprompt/Redesign |
| **Logic** | Poor CLI logging implementation | Redesign |
| **Logic** | Failed to correlate multiple signals |  **1-Shot Failure** |

**Pros:** I like the 3-task architecture (scan, receive, aggregate), and that it compiled with minor intervention.

**Cost:** < $0.01

### They Both Failed to 1-shot

There were a lot of issues with both attempts. One that I was saddened by was no that there was no attempt at writing tests. Yes you get what you ask for, but often with LLM's they hand out tests without even being asked. Not a huge deal but, I was curious, will an extremely widely used and well known best practice be given to me for free? The answer this time was "no".

My biggest gripe with both attempts was that I strongly hinted at a data structure to handle the correlation of bluetooth signals. One person may have multiple devices. A phone, headphones, smartwatch, car, I don't know? I do know that I didn't want to track a single bluetooth signal, I want to track the presence of "someone" or "something" by associating multiple bluetooth signals into a single identity in time. I did more than hint at this in the prompt, I said `This is the real goal, I want to identify vehicles or people by their bluetooth signatures as they frequent a location`. This isn't where the experiment ends. There is still a lot to discuss and learn here.


# Learning by cleaning up the DeepSeek result
I took some time to [clean](src/deepseek_cleaned_up.md) the result I obtained from DeepSeek. Making the code more legible. I wouldn't say this is production ready code. It cleaned up sort of OK. Here are my notes from that:

 - 213 lines of poorly organized code turned into 145 of better documented and more organized code. Reduction of ~33%. My goal wasn't to chop lines, you can read it for yourself, its just a first pass at making the code more maintainable.
 - I removed what I would consider to be 2 features (logging to file, aquiring the name of a device), because they don't serve the use case.
 - It took ~1 hr to do this while I rewatched some of lord of the rings. Thats a bit of time for such a small investment. It likely would have been easier to start from scratch. I think I would have gotten a better result and maybe rabbit trailed a bit to learn more about bluetooth.
 - The models seemed to spend considerable effort in areas I do not think a person would and far less effort in the areas I think a person would. The results are kind of weird despite almost having OK code.
 - It may have written the boiler plate but I don't know how much of it I would keep? 

# What does a Proof of Concept look like if I did it?
Seeing that the best result missed the point of the application, what might the final result look like if I kept going by hand?

I experimented for maybe 2-3 hrs with ideas for how to correlate devices to entities like people/vehicles. I started with some advanced time series modeling using signal strength aka `RSSI` and was going to have some fun. Then I settled on the thing I think a colleagues first guess would be. Why don't I just group everything I see in a batch as either belonging to a group, or becoming a new group? 

That experimentation involved me writing some data structures, testing with real devices, refactoring and iterating. When I look at the final result of this experimentation, which you can view [here](src/main.rs), I don't see much of the original code at all. Ignoring imports maybe 10 lines out of the now 250 (~4%) still exist. Am I in love with this stopping point for the project? No! If I really cared there is still work to do, there aren't even tests. I don't normally write code like this. However, this is good enough for an interesting follow up experiment.

# What does an LLM expect in a Prompt?
Let's pretend an agentic workflow or a person refined the prompt detailing the code over and over until it resulted in the proof of concept code I wrote by hand. I imagine if I added more words to the prompt I could get it close. Thats what people say right? Let's assume that the datastructure in the original prompt was too underspecified to succeed at the main goal and that is what held the attempt back. Let's generously call all of the issues here operator error at the expense of some personal gas lighting and admitting upfront that I didn't know much about the project.

So we have a final target now, the project can be fully specified, because I wrote up the code for the application I invisioned. Can we encode the neccesary items to reproduce this application into a prompt and then get a 1-shot? What if I wrote a really detailed prompt and made sure it used language that LLM's do well with. Could I ask an LLM to recreate something like my code from it. Should work. Right?

## DeepSeekV4 Pro Constructs Prompt from my Code and Asks Qwen to Generate it
- [Produced prompt](reconstruction_experiment/deepseek_prompt.md): ~750 words and 80 lines of text
- [Qwen produced](reconstruction_experiment/qwen36_reconstruction.md): 280 lines of code.

### Losses
- **Hallucinated methods** — *Doesn't build.*
- **Borrow checker & return type issues** — *Symptoms of fixes that take more than 1 min to address*
  
### Wins
- **`PresenceDetector` impl looks OK** — *We have some abstractions*
- **Status Update Concept** — *it's pretty good! I noodled over the idea myself and decided against it but its a reasonable thing to log. I prefer deltas but I see how in a complicated deployment you would want this.*

## Qwen3.6 35B A3B Constructs Prompt from my Code and Asks DeepSeek to Generate it 
- [Produced prompt](reconstruction_experiment/qwen_suggested_prompt.md): ~810 words and 89 lines of text
- [DeepSeek produced](reconstruction_experiment/deepseekv4_pro_reconstruction.md): 270 lines of code

### Losses
- **Awkward comment blocks & unused variables** — *negligable impact but awkward.*
- **New groups aren't reported as new** — *Makes understanding the logs harder and less greppable*
- **No methods in `PresenceDetector` struct** — *The grouping logic is owned by that type. This should be refactored*
- **The `analyze_batch` method is large** — *I appreciate the abstraction but this is unpleasant to read and maintain*
  
### Wins
- **It compiled** — *First successful 1-shot experiment*
- **Exception messages** — *The model included some exception handling messages which I didn't in my lazy version. Nice.*
- **Logs have structure** — *The println logs have timestamps. It's cute. We don't need that because we'll run this in SystemD, but I appreciate it.*

# Conclusions
## Technical / Operational 
 - The first result back from an LLM needs review. Just because something compiles or passes tests doesn't mean I want it in my code base or that it does what I asked for.
 - High-level takeaways from the first 1-shot experiments:
   - ~33% of the generated code was junk more or less. 
   - ~20% of the prompt was ignored despite multiple references to it being the most important feature. 
   - ~33% more code to get the baseline of what was asked into the project
   - In this case, I opted to rewrite about 95% of the generated code.
 - The thing that took the most time was not writing the code or debugging at all. It was experimenting to figure out what I actually needed to complete the project. Editting the implementation was a big part of that and was not an abstracted away concept like many people claim.
 - The LLM generated prompts which were **almost** successful recreations of the code that I wrote by hand required an immense amount of specification. 
   - The prompts had about as much words (750-810) as the actual code (775).  
   - The results still weren't great.
   - Decisions were made that I didn't have in the code, and they were decisions I considered in my implementation and decided against.
 - The best case result from a fully specified prompt, yielded somewhat sad results. 
   - There were both losses in requirements, poor code organization
   - The code felt bloated and yet lacked abstractions that would aid maintenance. 

## Personal 
 - Its really easy to squint my eyes and look at the code either model produced in either experiment and say "it's not that bad". In the first experiment it kind of almost did what I asked for and I didn't have to do much to get it(305 words in exchange for ~700 words). 
   - At the same time... I kept maybe 30 of those words. 
   - Again, rewriting this code was not the goal, but most of it was more or less required to add the missing features and make the code maintainable.
   - LLM code always seems kind of "plausible" at a first glance.
 - I tried to treat this like it was offloading a task to a colleague. This didn't match that experience, I know better from using LLM's in my day-to-day, but I wanted to treat it the way marketing and influencers report the chat experience to be. Coming in with an open mind lead me to disappointment but a better understanding. 
   - An exchange with an LLM isn't like dealing with a person even with technical output. There was a lot of effort put into weird places and not enough anywhere it mattered. 
   - A lot of people may have pushed back or asked questions before passing off what I recieved. That wasn't even an option.
   - Alternatively, they would have read what I wrote and went with it, like I did.
 - In some ways I appreciate the local model. If I hooked it up to a OpenCode/Hermes it probably would have been on par with deepseek's responses. But I am also glad I didn't. It was nice to have a reason to read documentation, even if it was because the model failed.
 - I learned very little about bluetooth. Thats somewhat typical when you use highlevel library, but for a hobby project that's a loss. 
   - I'm excited to go read about it! 
   - This experiment inverted the dopamine cycle I get from learning new things. Normally I read things, try stuff over and over, and then I get butterflys when something actually works and I understand it. This more or less handed me an app. A broken half-baked application that would stink to maintain, but an app nonetheless. 
   - I wonder how many people go back to learn things after something kind of *"seems to work"*? 
   - I also wonder how many people would accept the delivered item, maybe fix the surface level issues and run it thinking it was what they had asked for?
 - I tested my rewrite of the app using the agreed upon(2x) logic of the models. It seems to work, but I wouldn't be surprised if I could easily make it better by reading a little bit more.
 - I see the value in accelerated boiler plate, and basic tasks like writing the simple unit test cases for me with LLM's every day. Its great. I'm certainly viewing the value add of these entirely hands off approaches differently. 
   - I don't know if I would have caught everything I did if I were only acting as a reviewer of the code and not hands on iterating and testing things myself. 10 interactions like this, bite sized or not, could easily lead me into a weird design that sounded OK but I would certainly regret later.

## Extrapolating to Agentic Workflows?

I've experimented with them. Sometimes with success! Othertimes with what feels like several of these interactions compounded with some clean up along the way. I wouldn't claim these experiments directly translate or exponentially propagates the negative aspects of using these tools. Though I do wonder if the propagation of negative traits is at least linear? Or perhaps quadratic? It's hard to quantify, and I won't bother here, this is long enough. I would invite others to try similar experiments for themselves though.
