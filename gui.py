import tkinter as tk
import subprocess as sb
import time
import threading

from orchestration import *

def main():
    def label_text_change(text):
        label.config(text=text, fg='green')  

    window = tk.Tk()
    window.title("GUI")

    # Position GUI in the center of the screen
    screen_width = window.winfo_screenwidth()
    screen_height = window.winfo_screenheight()
    x = (screen_width // 2) - (1200 // 2)
    y = (screen_height // 2) - (600 // 2)
    # Configure the main window
    window.geometry(f'1200x600+{x}+{y}')  

    ### FRAMES
    top_frame = tk.Frame(window, width=200, height=60, bg='black')
    top_frame.pack()

    left_frame = tk.Frame(window, width=500, height=600)
    left_frame.pack(side="left", expand=True, fill="both")

    right_frame = tk.Frame(window, width=450, height=350, bg='lightblue')
    right_frame.pack(side="right", expand=True, anchor = "e", padx = 60, pady = 40)

    ### LABELS

    # Create a header label at the top
    header_label = tk.Label(top_frame, text="ADSDB Project", bg= 'black', font=('Roboto', 16), fg='white')
    header_label.place(relwidth=1, relheight=1)

    # Create a label with initial text color and background
    label = tk.Label(right_frame, text="", font=('Roboto', 15),
                     bg='lightblue', borderwidth=2, wraplength=300)
    label.place(relx=0.5, rely=0.5, anchor="center")

    ### BUTTONS

    # Frame for positioniing the buttons
    button_frame = tk.Frame(left_frame)
    button_frame.place(relx=0.35, rely=0.5, anchor="center")

    button_tem = tk.Button(button_frame, text="Temporal Landing Zone", command=lambda : {
        label_text_change(data_ingester())
        })
    button_tem.pack(pady = 7)

    button_per = tk.Button(button_frame, text="Persistent Landing Zone", command=lambda : {
        label_text_change(persistent_loader())
        })
    button_per.pack(pady = 7)

    button_for = tk.Button(button_frame, text="Formatted Zone", command=lambda : {
        label_text_change(formatted_loader())
        })
    button_for.pack(pady = 7)

    button_tru = tk.Button(button_frame, text="Trusted Zone", command=lambda : {
        label_text_change(trusted_loader())
        })
    button_tru.pack(pady = 7)

    button_exp = tk.Button(button_frame, text="Exploitation Zone", command=lambda : {
        label_text_change(exploitation_loader())
        })
    button_exp.pack(pady = 7)

    # Frame for positioning the buttons for part 2
    button_frame2 = tk.Frame(left_frame)
    button_frame2.place(relx=0.75, rely=0.41, anchor="center")

    button_san = tk.Button(button_frame2, text="Analytical Sandbox", command=lambda : {
        label_text_change(sandbox_loader())
        })
    button_san.pack(pady = 7)

    button_fe = tk.Button(button_frame2, text="Feature Engineering", command=lambda : {
        label_text_change(feature_enginering_loader())
        })
    button_fe.pack(pady = 7)

    button_dp = tk.Button(button_frame2, text="Data Preparation", command=lambda : {
        label_text_change(data_preparation_loader())
        })
    button_dp.pack(pady = 7)

    button_model = tk.Button(button_frame2, text="Model Training", command=lambda : {
        label_text_change(model_training_loader())
        })
    button_model.pack(pady = 14)

    ### MONITORING PERFORMANCE

    cpu_label = tk.Label(button_frame, font=('Roboto', 12), fg='blue', wraplength=500)
    cpu_label.pack(pady = 7)
        
    memory_label = tk.Label(button_frame, font=('Roboto', 12), fg='green', wraplength=500)
    memory_label.pack(pady = 7)

    # Monitoring in a thread
    th = threading.Thread(target=sys_monitor,  args=(window, cpu_label, memory_label))
    th.daemon = True
    th.start()

    window.mainloop()

# Monitoring system performance
def sys_monitor(window, cpu_label, memory_label):
    while True:
        process = sb.Popen(['top', '-l', '1', '-n', '0'], stdout=sb.PIPE, text=True)
        output, _ = process.communicate()
        
        # Parsing output to find CPU and memory usage
        lines = output.split('\n')
        cpu_ln = next((line for line in lines if "CPU usage" in line), None)
        memory_ln = next((line for line in lines if "PhysMem" in line), None)

        cpu_usage = cpu_ln.replace('CPU usage:', '').strip()
        memory_usage = memory_ln.replace('PhysMem:', '').split(',')[0].strip()  # Simplified to first comma

        # Cleaning
        if cpu_usage.endswith('%'):
            cpu_usage = cpu_usage[:-1]
        memory_usage = memory_usage[:-1]
        memory_usage += ')' 

        # Ensure UI updates happen on the main thread
        window.after(100, lambda: cpu_label.config(text=f"CPU Usage: {cpu_usage}"))
        window.after(100, lambda: memory_label.config(text=f"Memory Usage: {memory_usage}"))

        # FOR POTENTIAL MEASURE OF PERFORMANCE OF JUST THE PYTHON EXECUTION SCRIPT
        # result = sb.run(['ps', '-A', '-o', '%cpu,comm,rss', '|', 'grep', process_name], capture_output=True, text=True)
        # output = result.stdout
        # cpu_usage = output.split()[0]  # Assuming %CPU is the first column
        # memory_usage = output.split()[2]  # Assuming RSS is the third column
        # window.after(0, cpu_label.config, {'text': cpu_text})
        # window.after(0, memory_label.config, {'text': memory_text})
            
        # Update every 5 seconds
        time.sleep(5)

if __name__ == "__main__":
    main()